#include "minispark.h"
#include "lib.h"
#include <pthread.h>
#include <unistd.h>
#include <sched.h>
#include <stdarg.h>
#include <string.h>
#include <sys/sysinfo.h>

#define _GNU_SOURCE
#define DEBUG(fmt, ...) fprintf(stderr, "[%s:%d] " fmt "\n", __func__, __LINE__, ##__VA_ARGS__) 
void* identity(void* arg);

typedef struct {
    pthread_t* threads;
    int num_threads;
    List* work_queue;
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_cond;
    int shutdown;
    int active_tasks;
    pthread_mutex_t active_lock;
    pthread_cond_t active_cond;
} ThreadPool;

static struct row* parse_row(char* line) {
    struct row* r = malloc(sizeof(struct row));
    r->ncols = 0;

    char* tok = strtok(line, "\t");
    while (tok && r->ncols < MAXCOLS) {
        strncpy(r->cols[r->ncols], tok, MAXLEN);  
        r->ncols++;
        tok = strtok(NULL, "\t");
    }

    return r;
}

static ThreadPool* thread_pool = NULL;
static List* metric_queue = NULL;
static pthread_mutex_t metric_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t metric_cond = PTHREAD_COND_INITIALIZER;
static pthread_t metric_thread;
static FILE* metric_file = NULL;
static void free_row(struct row* r);  

static void free_row(struct row* r) {
    free(r);  
}

List* list_init(int initial_capacity) {
    /* DEBUG("Creating list with capacity %d", initial_capacity); */
    List* l = malloc(sizeof(List));
    l->elements = malloc(initial_capacity * sizeof(void*));
    l->size = 0;
    l->capacity = initial_capacity;
    return l;
}

void list_add_elem(List* l, void* elem) {
    if (!l) {
        /* DEBUG("Attempt to add to NULL list"); */
        return;
    }
    if (l->size >= l->capacity) {
        /* DEBUG("Resizing list from %d to %d", l->capacity, l->capacity * 2); */
        l->capacity *= 2;
        l->elements = realloc(l->elements, l->capacity * sizeof(void*));
    }
    /* DEBUG("Adding element %p (list size now %d)", elem, l->size + 1); */
    l->elements[l->size++] = elem;
}

void* list_get_elem(List* l, int index) {
    if (!l) {
        /* DEBUG("Attempt to get from NULL list"); */
        return NULL;
    }
    if (index < 0 || index >= l->size) {
        /* DEBUG("Invalid index %d (size=%d)", index, l->size); */
        return NULL;
    }
    /* DEBUG("Returning element %d: %p", index, l->elements[index]); */
    return l->elements[index];
}

void* list_remove_first(List* l) {
    if (!l) {
        /* DEBUG("Attempt to remove from NULL list"); */
        return NULL;
    }
    if (l->size == 0) {
        /* DEBUG("Attempt to remove from empty list"); */
        return NULL;
    }
    void* elem = l->elements[0];
    /* DEBUG("Removing first element %p (size was %d)", elem, l->size); */
    for (int i = 1; i < l->size; i++) {
        l->elements[i-1] = l->elements[i];
    }
    l->size--;
    return elem;
}

void list_set_elem(List* l, int index, void* elem) {
    if (!l) {
        /* DEBUG("Attempt to set in NULL list"); */
        return;
    }
    if (index < 0 || index >= l->capacity) {
        /* DEBUG("Invalid index %d (capacity=%d)", index, l->capacity); */
        return;
    }
    /* DEBUG("Setting element %d to %p (size was %d)", index, elem, l->size); */
    if (index >= l->size) l->size = index + 1;
    l->elements[index] = elem;
}

int list_size(List* l) {
    if (!l) {
        /* DEBUG("NULL list size requested"); */
        return 0;
    }
    /* DEBUG("List size: %d", l->size); */
    return l->size;
}

void list_free(List* l) {
    if (!l) {
        /* DEBUG("Attempt to free NULL list"); */
        return;
    }
    /* DEBUG("Freeing list with %d elements", l->size); */
    free(l->elements);
    free(l);
}

static void* worker_thread(void* arg) {
    /* DEBUG("Worker thread starting"); */
    while (1) {
        pthread_mutex_lock(&thread_pool->queue_lock);
        /* DEBUG("Worker acquired queue lock"); */
        
        while (!thread_pool->shutdown && list_size(thread_pool->work_queue) == 0) {
            /* DEBUG("Worker waiting for work"); */
            pthread_cond_wait(&thread_pool->queue_cond, &thread_pool->queue_lock);
            /* DEBUG("Worker woke up"); */
        }

        if (thread_pool->shutdown) {
            /* DEBUG("Worker shutting down"); */
            pthread_mutex_unlock(&thread_pool->queue_lock);
            pthread_exit(NULL);
        }

        Task* task = list_remove_first(thread_pool->work_queue);
        /* DEBUG("Worker got task for RDD %p partition %d (queue size now %d)", task->rdd, task->pnum, list_size(thread_pool->work_queue)); */
        pthread_mutex_unlock(&thread_pool->queue_lock);

        clock_gettime(CLOCK_MONOTONIC, &task->metric->scheduled);
        /* DEBUG("Task scheduled at %ld.%09ld", task->metric->scheduled.tv_sec, task->metric->scheduled.tv_nsec); */

        if (task->rdd && !list_get_elem(task->rdd->partitions, task->pnum)) {
            List* partition = list_init(16);
            /* DEBUG("Created new partition %p for RDD %p partition %d", partition, task->rdd, task->pnum); */
            list_set_elem(task->rdd->partitions, task->pnum, partition);

            if (task->rdd->trans == MAP) {
                RDD* dep = task->rdd->dependencies[0];
                List* dep_partition = list_get_elem(dep->partitions, task->pnum);
                if (dep->trans == FILE_BACKED) {
                    //DEBUG("Processing FILE_BACKED dependency");
                    if (!dep_partition) {
                        DEBUG("ERROR: dep_partition is NULL");
                        continue;
                    }

                    
                    FILE* fp = list_get_elem(dep_partition, 0);
                    if (!fp) {
                        //DEBUG("ERROR: FILE* is NULL in dep_partition");
                        continue;
                    }

                    rewind(fp);
                    
                    if (task->rdd->fn == (void*)identity) {
                        //DEBUG("Processing identity mapper (line-by-line)");
                        char* line = NULL;
                        size_t len = 0;
                        while (getline(&line, &len, fp) != -1) {
                            line[strcspn(line, "\n")] = 0;
                            void* line_copy = strdup(line);
                            if (line_copy) list_add_elem(partition, line_copy);
                        }
                        free(line);
                    } else {
                        //DEBUG("Processing whole-file mapper");
                        FILE* fp_copy = fdopen(dup(fileno(fp)), "r");
                        if (!fp_copy) {
                            DEBUG("ERROR: fdopen failed");
                            continue;
                        }
                        void* mapped = ((Mapper)task->rdd->fn)(fp_copy);
                        if (mapped) list_add_elem(partition, mapped);
                        fclose(fp_copy);
                    }
                } else {
                    // Original non-file-backed processing
                    for (int i = 0; i < list_size(dep_partition); i++) {
                        void* elem = list_get_elem(dep_partition, i);
                        void* mapped = ((Mapper)task->rdd->fn)(elem);
                        if (mapped) list_add_elem(partition, mapped);
                    }
                }
            }

            /*if (task->rdd->trans == MAP) {
                RDD* dep = task->rdd->dependencies[0];
                List* dep_partition = list_get_elem(dep->partitions, task->pnum);
                // DEBUG("Processing MAP on RDD %p partition %d (dep RDD %p partition %p)", 
                      task->rdd, task->pnum, dep, dep_partition); 
                
                if (dep->trans == FILE_BACKED) {
                    FILE* fp = list_get_elem(dep_partition, 0);
                    // DEBUG("Processing FILE_BACKED partition with FILE* %p", fp); 
                    rewind(fp);
                    char* line = NULL;
                    size_t len = 0;
                    ssize_t read;
                    
                    while ((read = getline(&line, &len, fp)) != -1) {
                        // Remove newline and ensure proper tab separation
                        line[strcspn(line, "\n")] = 0;
                        // Replace spaces with tabs to ensure consistent splitting
                        //for (char* p = line; *p; p++) {
                          //  if (*p == ' ') *p = '\t';
                        //}
                        void* line_copy = strdup(line);
                        if (line_copy) {
                            // DEBUG("Adding line copy %p: %s", line_copy, (char*)line_copy); 
                            list_add_elem(partition, line_copy);
                        }
                    }
                    free(line);
                    // DEBUG("Finished reading file (partition now has %d elements)", list_size(partition)); 
                } else {
                    // Existing map processing for non-file-backed RDDs
                    for (int i = 0; i < list_size(dep_partition); i++) {
                        void* elem = list_get_elem(dep_partition, i);
                        void* mapped = ((Mapper)task->rdd->fn)(elem);
                        if (mapped) {
                            list_add_elem(partition, mapped);
                        }
                    }
                }
            }*/
            
            if (task->rdd->trans == FILTER) {
                RDD* dep = task->rdd->dependencies[0];
                List* dep_partition = list_get_elem(dep->partitions, task->pnum);
                List* partition = list_init(16);
            
                for (int i = 0; i < list_size(dep_partition); i++) {
                    void* elem = list_get_elem(dep_partition, i);
            
                    if (task->rdd->fn == StringContains && elem != NULL) {
                        ((char*)elem)[strcspn((char*)elem, "\n")] = 0;
                    }
            
                    if (((Filter)task->rdd->fn)(elem, task->rdd->ctx)) {
                        list_add_elem(partition, elem);
                    }
                }
            
                list_set_elem(task->rdd->partitions, task->pnum, partition);
            } 
            
            if (task->rdd->trans == JOIN) {
                RDD* left_rdd = task->rdd->dependencies[0];
                RDD* right_rdd = task->rdd->dependencies[1];
                /* DEBUG("Processing JOIN on RDD %p partition %d (left RDD %p, right RDD %p)", 
                      task->rdd, task->pnum, left_rdd, right_rdd); */
                
                List* left_partition = list_get_elem(left_rdd->partitions, task->pnum);
                List* right_partition = list_get_elem(right_rdd->partitions, task->pnum);
                
                if (!left_partition || !right_partition) {
                    /* DEBUG("ERROR: Missing input partitions for join (left: %p, right: %p)",
                          left_partition, right_partition); */
                    continue;
                }
                
                List* output_partition = list_init(16);
                /* DEBUG("Left partition size: %d, Right partition size: %d", 
                      list_size(left_partition), list_size(right_partition)); */

                for (int i = 0; i < list_size(left_partition); i++) {
                    void* left_row = list_get_elem(left_partition, i);
                    /* DEBUG("Left row %d: %p", i, left_row); */
                    for (int j = 0; j < list_size(right_partition); j++) {
                        void* right_row = list_get_elem(right_partition, j);
                        /* DEBUG("Right row %d: %p", j, right_row); */
                        void* joined = ((Joiner)task->rdd->fn)(left_row, right_row, task->rdd->ctx);
                        /* DEBUG("Join result: %p", joined); */
                        if (joined != NULL) {
                            list_add_elem(output_partition, joined);
                        }
                    }
                }
                
                list_set_elem(task->rdd->partitions, task->pnum, output_partition);
                
                // Free input partitions as per project requirements
                list_free(left_partition);
                list_free(right_partition);
                list_set_elem(left_rdd->partitions, task->pnum, NULL);
                list_set_elem(right_rdd->partitions, task->pnum, NULL);
            }
        }

        struct timespec end_time;
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        task->metric->duration = TIME_DIFF_MICROS(task->metric->scheduled, end_time);
        /* DEBUG("Task completed in %ld microseconds", task->metric->duration); */

        pthread_mutex_lock(&metric_lock);
        list_add_elem(metric_queue, task->metric);
        pthread_cond_signal(&metric_cond);
        pthread_mutex_unlock(&metric_lock);

        pthread_mutex_lock(&thread_pool->active_lock);
        thread_pool->active_tasks--;
        /* DEBUG("Active tasks remaining: %d", thread_pool->active_tasks); */
        if (thread_pool->active_tasks == 0) {
            /* DEBUG("Signaling completion of all tasks"); */
            pthread_cond_signal(&thread_pool->active_cond);
        }
        pthread_mutex_unlock(&thread_pool->active_lock);

        free(task);
    }
    return NULL;
}

static void* metric_thread_func(void* arg) {
    /* DEBUG("Metric thread starting"); */
    metric_file = fopen("metrics.log", "w");
    if (!metric_file) {
        /* DEBUG("Failed to open metrics.log"); */
        return NULL;
    }

    while (1) {
        pthread_mutex_lock(&metric_lock);
        /* DEBUG("Metric thread checking queue (size=%d)", list_size(metric_queue)); */
        while (!thread_pool->shutdown && list_size(metric_queue) == 0) {
            /* DEBUG("Metric thread waiting"); */
            pthread_cond_wait(&metric_cond, &metric_lock);
            /* DEBUG("Metric thread woke up"); */
        }

        if (thread_pool->shutdown && list_size(metric_queue) == 0) {
            /* DEBUG("Metric thread shutting down"); */
            pthread_mutex_unlock(&metric_lock);
            break;
        }

        TaskMetric* metric = list_remove_first(metric_queue);
        /* DEBUG("Processing metric for RDD %p partition %d", metric->rdd, metric->pnum); */
        pthread_mutex_unlock(&metric_lock);

        print_formatted_metric(metric, metric_file);
        free(metric);
    }

    fclose(metric_file);
    /* DEBUG("Metric thread exiting"); */
    return NULL;
}

void print_formatted_metric(TaskMetric* metric, FILE* fp) {
    /* DEBUG("Writing metric to file"); */
    fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
            metric->rdd, metric->pnum, metric->rdd->trans,
            metric->created.tv_sec, metric->created.tv_nsec / 1000,
            metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
            metric->duration);
}

void MS_Run() {
    /* DEBUG("Initializing MiniSpark"); */
    int num_threads = get_nprocs();
    /* DEBUG("Creating thread pool with %d threads", num_threads); */
    
    thread_pool = malloc(sizeof(ThreadPool));
    thread_pool->threads = malloc(num_threads * sizeof(pthread_t));
    thread_pool->num_threads = num_threads;
    thread_pool->work_queue = list_init(16);
    thread_pool->shutdown = 0;
    thread_pool->active_tasks = 0;
    pthread_mutex_init(&thread_pool->queue_lock, NULL);
    pthread_cond_init(&thread_pool->queue_cond, NULL);
    pthread_mutex_init(&thread_pool->active_lock, NULL);
    pthread_cond_init(&thread_pool->active_cond, NULL);

    for (int i = 0; i < num_threads; i++) {
        /* DEBUG("Creating worker thread %d", i); */
        pthread_create(&thread_pool->threads[i], NULL, worker_thread, NULL);
    }

    metric_queue = list_init(16);
    /* DEBUG("Creating metric thread"); */
    pthread_create(&metric_thread, NULL, metric_thread_func, NULL);
    /* DEBUG("MiniSpark initialization complete"); */
}

void MS_TearDown() {
    if (!thread_pool) {
        /* DEBUG("Thread pool already NULL"); */
        return;
    }

    /* DEBUG("Initiating shutdown sequence"); */
    
    pthread_mutex_lock(&thread_pool->queue_lock);
    thread_pool->shutdown = 1;
    /* DEBUG("Broadcasting shutdown to workers"); */
    pthread_cond_broadcast(&thread_pool->queue_cond);
    pthread_mutex_unlock(&thread_pool->queue_lock);

    pthread_mutex_lock(&metric_lock);
    /* DEBUG("Signaling metric thread to shutdown"); */
    pthread_cond_signal(&metric_cond);
    pthread_mutex_unlock(&metric_lock);

    /* DEBUG("Joining worker threads"); */
    for (int i = 0; i < thread_pool->num_threads; i++) {
        /* DEBUG("Joining worker thread %d", i); */
        pthread_join(thread_pool->threads[i], NULL);
    }

    /* DEBUG("Joining metric thread"); */
    pthread_join(metric_thread, NULL);

    /* DEBUG("Cleaning up resources"); */
    list_free(thread_pool->work_queue);
    free(thread_pool->threads);
    pthread_mutex_destroy(&thread_pool->queue_lock);
    pthread_cond_destroy(&thread_pool->queue_cond);
    pthread_mutex_destroy(&thread_pool->active_lock);
    pthread_cond_destroy(&thread_pool->active_cond);
    free(thread_pool);
    thread_pool = NULL;

    while (list_size(metric_queue)) {
        free(list_remove_first(metric_queue));
    }
    list_free(metric_queue);
    metric_queue = NULL;
    
    /* DEBUG("Shutdown complete"); */
}

void execute(RDD* rdd) {
    /* DEBUG("Executing RDD %p (transformation %d)", rdd, rdd->trans); */
    if (!rdd) return;

    for (int i = 0; i < rdd->numdependencies; i++) {
        /* DEBUG("Materializing dependency %d for RDD %p", i, rdd); */
        execute(rdd->dependencies[i]);
    }

    if (rdd->trans == JOIN && rdd->numdependencies == 2) {
        RDD* left = rdd->dependencies[0];
        RDD* right = rdd->dependencies[1];
        if (list_size(left->partitions) != list_size(right->partitions)) {
            /* DEBUG("JOIN ERROR: Partition count mismatch (%d vs %d)", 
                  list_size(left->partitions), list_size(right->partitions)); */
            return;
        }
    }

    if (!rdd->partitions) {
        int num_partitions = 1;
        
        if (rdd->trans == PARTITIONBY) {
            num_partitions = rdd->numpartitions;
        } 
        else if (rdd->numdependencies > 0) {
            num_partitions = list_size(rdd->dependencies[0]->partitions);
        }

        /* DEBUG("Creating %d partitions for RDD %p", num_partitions, rdd); */
        rdd->partitions = list_init(num_partitions);
        for (int i = 0; i < num_partitions; i++) {
            list_add_elem(rdd->partitions, NULL);
            /* DEBUG("Added partition %d (size now %d)", i, list_size(rdd->partitions)); */
        }
    }

    pthread_mutex_lock(&thread_pool->active_lock);
    thread_pool->active_tasks = list_size(rdd->partitions);
    /* DEBUG("Set active tasks to %d for RDD %p", thread_pool->active_tasks, rdd); */
    pthread_mutex_unlock(&thread_pool->active_lock);

    for (int i = 0; i < list_size(rdd->partitions); i++) {
        Task* task = malloc(sizeof(Task));
        task->rdd = rdd;
        task->pnum = i;
        task->metric = malloc(sizeof(TaskMetric));
        clock_gettime(CLOCK_MONOTONIC, &task->metric->created);
        task->metric->rdd = rdd;
        task->metric->pnum = i;

        pthread_mutex_lock(&thread_pool->queue_lock);
        list_add_elem(thread_pool->work_queue, task);
        /* DEBUG("Added task for partition %d (queue size now %d)", i, list_size(thread_pool->work_queue)); */
        pthread_cond_signal(&thread_pool->queue_cond);
        pthread_mutex_unlock(&thread_pool->queue_lock);
    }

    pthread_mutex_lock(&thread_pool->active_lock);
    while (thread_pool->active_tasks > 0) {
        /* DEBUG("Waiting for %d tasks to complete for RDD %p", thread_pool->active_tasks, rdd); */
        pthread_cond_wait(&thread_pool->active_cond, &thread_pool->active_lock);
    }
    pthread_mutex_unlock(&thread_pool->active_lock);
    /* DEBUG("All tasks completed for RDD %p", rdd); */
}

int count(RDD* rdd) {
    /* DEBUG("Counting elements in RDD %p", rdd); */
    execute(rdd);
    int count = 0;
    for (int i = 0; i < list_size(rdd->partitions); i++) {
        List* partition = list_get_elem(rdd->partitions, i);
        count += list_size(partition);
        /* DEBUG("Partition %d has %d elements (total now %d)", i, list_size(partition), count); */
    }
    /* DEBUG("Final count for RDD %p: %d", rdd, count); */
    return count;
}

void print(RDD* rdd, Printer p) {
    /* DEBUG("Printing RDD %p", rdd); */
    execute(rdd);
    
    if (!rdd->partitions) {
        /* DEBUG("No partitions to print"); */
        return;
    }

    // Track if we need to add newlines (for string output)
    int is_string_output = (rdd->trans == MAP && rdd->dependencies[0]->trans == FILE_BACKED);
    int is_filter = rdd->trans == FILTER;

    for (int i = 0; i < list_size(rdd->partitions); i++) {
        List* partition = list_get_elem(rdd->partitions, i);
        if (!partition) {
            /* DEBUG("Partition %d is NULL", i); */
            continue;
        }
        
        /* DEBUG("Printing partition %d with %d elements", i, list_size(partition)); */
             
        for (int j = 0; j < list_size(partition); j++) {
            void* elem = list_get_elem(partition, j);
            if (elem) {
                p(elem);
                // Only add newline for simple string output
                //if (is_string_output || is_filter) {
                  //  printf("\n");
                //}
                free(elem);
            }
        }
        list_free(partition);
    }
    list_free(rdd->partitions);
    rdd->partitions = NULL;
    /* DEBUG("Finished printing RDD %p", rdd); */
}

RDD* create_rdd(int numdeps, Transform t, void* fn, ...) {
    /* DEBUG("Creating RDD with %d dependencies, transform %d", numdeps, t); */
    RDD* rdd = malloc(sizeof(RDD));
    memset(rdd, 0, sizeof(RDD));
    rdd->trans = t;
    rdd->fn = fn;

    va_list args;
    va_start(args, fn);
    for (int i = 0; i < numdeps; i++) {
        rdd->dependencies[i] = va_arg(args, RDD*);
        /* DEBUG("Added dependency %d: RDD %p", i, rdd->dependencies[i]); */
    }
    va_end(args);
    rdd->numdependencies = numdeps;
    /* DEBUG("Created RDD %p", rdd); */
    return rdd;
}

RDD* map(RDD* dep, Mapper fn) {
    /* DEBUG("Creating MAP RDD from %p", dep); */
    return create_rdd(1, MAP, fn, dep);
}

RDD* filter(RDD* dep, Filter fn, void* ctx) {
    /* DEBUG("Creating FILTER RDD from %p", dep); */
    RDD* rdd = create_rdd(1, FILTER, fn, dep);
    rdd->ctx = ctx;
    return rdd;
}

RDD* partitionBy(RDD* dep, Partitioner fn, int numpartitions, void* ctx) {
    /* DEBUG("Creating PARTITIONBY RDD from %p (%d partitions)", dep, numpartitions); */
    RDD* rdd = create_rdd(1, PARTITIONBY, fn, dep);
    rdd->numpartitions = numpartitions;
    rdd->ctx = ctx;
    return rdd;
}

RDD* join(RDD* dep1, RDD* dep2, Joiner fn, void* ctx) {
    /* DEBUG("Creating JOIN RDD from %p and %p", dep1, dep2); */
    RDD* rdd = create_rdd(2, JOIN, fn, dep1, dep2);
    rdd->ctx = ctx;
    return rdd;
}

void* identity(void* arg) {
    /* DEBUG("Identity function called with %p", arg); */
    return arg;
}

RDD* RDDFromFiles(char** filenames, int numfiles) {
    /* DEBUG("Creating FILE_BACKED RDD from %d files", numfiles); */
    RDD* rdd = malloc(sizeof(RDD));
    memset(rdd, 0, sizeof(RDD));
    rdd->partitions = list_init(numfiles);
    rdd->trans = FILE_BACKED;
    rdd->fn = (void*)identity;

    for (int i = 0; i < numfiles; i++) {
        /* DEBUG("Opening file %s", filenames[i]); */
        FILE* fp = fopen(filenames[i], "r");
        if (!fp) {
            perror("fopen");
            for (int j = 0; j < i; j++) {
                List* part = list_get_elem(rdd->partitions, j);
                if (part) {
                    fclose(list_get_elem(part, 0));
                    list_free(part);
                }
            }
            list_free(rdd->partitions);
            free(rdd);
            return NULL;
        }
        
        List* partition = list_init(1);
        list_add_elem(partition, fp);
        list_set_elem(rdd->partitions, i, partition);
        /* DEBUG("Added file %s as partition %d", filenames[i], i); */
    }
    /* DEBUG("Created FILE_BACKED RDD %p with %d partitions", rdd, numfiles); */
    return rdd;
}