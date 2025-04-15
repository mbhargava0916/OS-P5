#include "minispark.h"
#include "lib.h"
#include <pthread.h>
#include <unistd.h>
#include <sched.h>
#include <stdarg.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <stdint.h>

#define _GNU_SOURCE
#define DEBUG(fmt, ...) fprintf(stderr, "[%s:%d] " fmt "\n", __func__, __LINE__, ##__VA_ARGS__) 

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

void* identity(void* arg) {
    /* DEBUG("Identity function called with %p", arg); */
    return arg;
}

static void* worker_thread(void* arg) {
    (void)arg;  // Mark arg as unused
    while (1) {
        pthread_mutex_lock(&thread_pool->queue_lock);
        while (!thread_pool->shutdown && list_size(thread_pool->work_queue) == 0) {
            pthread_cond_wait(&thread_pool->queue_cond, &thread_pool->queue_lock);
        }

        if (thread_pool->shutdown) {
            pthread_mutex_unlock(&thread_pool->queue_lock);
            pthread_exit(NULL);
        }

        Task* task = list_remove_first(thread_pool->work_queue);
        pthread_mutex_unlock(&thread_pool->queue_lock);
        clock_gettime(CLOCK_MONOTONIC, &task->metric->scheduled);

        if (task->rdd && !list_get_elem(task->rdd->partitions, task->pnum)) {
            List* partition = list_init(16);
            list_set_elem(task->rdd->partitions, task->pnum, partition);

            if (task->rdd->trans == MAP) {
                RDD* dep = task->rdd->dependencies[0];
                List* dep_partition = list_get_elem(dep->partitions, task->pnum);

                if (dep->trans == FILE_BACKED) {
                    void* first_elem = list_get_elem(dep_partition, 0);
                    
                    if( task->rdd->fn == (void*)GetLines) {
                        //DEBUG("task->rdd->fn: %p, identity: %p", task->rdd->fn, (void*)identity);
                        FILE* fp = (FILE*)first_elem;
                        rewind(fp);
                        char* line = NULL;
                        size_t len = 0;
                        while (getline(&line, &len, fp) != -1) {
                            line[strcspn(line, "\n")] = 0;
                            void* line_copy = strdup(line);
                            if (line_copy){
                                list_add_elem(partition, line_copy);
                            } 
                        }
                        free(line);
                    }else{
                        FILE* fp = (FILE*)first_elem;
                        FILE* fp_copy = fdopen(dup(fileno(fp)), "r");
                        if (!fp_copy) continue;
                
                        void* mapped = ((Mapper)task->rdd->fn)(fp_copy);
                        if (mapped) list_add_elem(partition, mapped);
                        fclose(fp_copy);
                    }
                } else {
                    for (int i = 0; i < list_size(dep_partition); i++) {
                        void* elem = list_get_elem(dep_partition, i);
                        void* mapped = ((Mapper)task->rdd->fn)(elem);
                        if (mapped) list_add_elem(partition, mapped);
                    }
                }
            

                /*for (int i = 0; i < list_size(dep_partition); i++) {
                    void* elem = list_get_elem(dep_partition, i);
                    void* mapped = ((Mapper)task->rdd->fn)(elem);
                    if (mapped) list_add_elem(partition, mapped);
                }*/


            }

            if (task->rdd->trans == FILTER) {
                RDD* dep = task->rdd->dependencies[0];
                List* dep_partition = list_get_elem(dep->partitions, task->pnum);
                List* filtered = list_init(16);

                for (int i = 0; i < list_size(dep_partition); i++) {
                    void* elem = list_get_elem(dep_partition, i);

                    if (task->rdd->fn == StringContains && elem != NULL) {
                        ((char*)elem)[strcspn((char*)elem, "\n")] = 0;
                    }

                    if (((Filter)task->rdd->fn)(elem, task->rdd->ctx)) {
                        list_add_elem(filtered, elem);
                    }
                }

                list_set_elem(task->rdd->partitions, task->pnum, filtered);
            }

            if (task->rdd->trans == JOIN) {
                RDD* left_rdd = task->rdd->dependencies[0];
                RDD* right_rdd = task->rdd->dependencies[1];

                List* left_partition = list_get_elem(left_rdd->partitions, task->pnum);
                List* right_partition = list_get_elem(right_rdd->partitions, task->pnum);
                if (!left_partition || !right_partition) continue;

                List* output_partition = list_init(16);
                for (int i = 0; i < list_size(left_partition); i++) {
                    void* left_row = list_get_elem(left_partition, i);
                    for (int j = 0; j < list_size(right_partition); j++) {
                        void* right_row = list_get_elem(right_partition, j);
                        void* joined = ((Joiner)task->rdd->fn)(left_row, right_row, task->rdd->ctx);
                        if (joined) list_add_elem(output_partition, joined);
                    }
                }

                list_set_elem(task->rdd->partitions, task->pnum, output_partition);
                list_free(left_partition);
                list_free(right_partition);
                list_set_elem(left_rdd->partitions, task->pnum, NULL);
                list_set_elem(right_rdd->partitions, task->pnum, NULL);
            }
        }

        /*if (task->rdd->trans == PARTITIONBY) {
            RDD* dep = task->rdd->dependencies[0];
            int num_parts = ((ExtendedRDD*)task->rdd)->numpartitions;
            List** outputs = malloc(sizeof(List*) * num_parts);
            for (int i = 0; i < num_parts; i++) {
                outputs[i] = list_init(10);
            }

            pthread_mutex_t* locks = malloc(sizeof(pthread_mutex_t) * num_parts);
            for (int j = 0; j < num_parts; j++) {
                pthread_mutex_init(&locks[j], NULL);
            }

            int np = list_size(dep->partitions);
            for (int i = 0; i < np; i++) {
                List* in_part = list_get_elem(dep->partitions, i);
                PartitionByTaskArg* arg = malloc(sizeof(PartitionByTaskArg));
                arg->header.rdd = task->rdd;
                arg->header.pnum = i;
                arg->input = in_part;
                arg->outputs = outputs;
                arg->num_outputs = num_parts;
                arg->partitioner = (Partitioner)task->rdd->fn;
                arg->ctx = task->rdd->ctx;
                arg->locks = locks;
                thread_pool_submit(process_partitionby_task, arg);
            }


            for (int j = 0; j < num_parts; j++) {
                pthread_mutex_destroy(&locks[j]);
            }
            free(locks);

            List* new_parts = list_init(num_parts);
            for (int i = 0; i < num_parts; i++) {
                list_add_elem(new_parts, outputs[i]);
            }
            free(outputs);
            task->rdd->partitions = new_parts;
            return;
        }*/
    

        struct timespec end_time;
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        task->metric->duration = TIME_DIFF_MICROS(task->metric->scheduled, end_time);

        pthread_mutex_lock(&metric_lock);
        list_add_elem(metric_queue, task->metric);
        pthread_cond_signal(&metric_cond);
        pthread_mutex_unlock(&metric_lock);

        pthread_mutex_lock(&thread_pool->active_lock);
        thread_pool->active_tasks--;
        if (thread_pool->active_tasks == 0) {
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
    // Map transformation types to their numeric values explicitly
    int trans_type;
    switch(metric->rdd->trans) {
        case MAP: trans_type = 0; break;
        case FILTER: trans_type = 1; break;
        case JOIN: trans_type = 2; break;
        case PARTITIONBY: trans_type = 3; break;
        case FILE_BACKED: trans_type = 4; break;
        default: trans_type = -1;  // Should never happen
    }
    
    fprintf(fp, 
        "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
        (void*)metric->rdd,
        metric->pnum,
        trans_type,
        (intmax_t)metric->created.tv_sec,
        metric->created.tv_nsec / 1000,
        (intmax_t)metric->scheduled.tv_sec,
        metric->scheduled.tv_nsec / 1000,
        metric->duration
    );
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

void thread_pool_submit(void (*func)(void*), void* arg) {
    // Direct call since your thread pool is used only for MAP/FILTER/JOIN
    func(arg);
}

void process_partitionby_task(void* arg) {
    PartitionByTaskArg* task = (PartitionByTaskArg*)arg;

    for (int i = 0; i < list_size(task->input); i++) {
        void* elem = list_get_elem(task->input, i);
        int part = task->partitioner(elem, task->num_outputs, task->ctx);

        pthread_mutex_lock(&task->locks[part]);
        list_add_elem(task->outputs[part], elem);
        pthread_mutex_unlock(&task->locks[part]);
    }

    free(task);
}


void execute(RDD* rdd) {
    if (!rdd) return;

    for (int i = 0; i < rdd->numdependencies; i++) {
        execute(rdd->dependencies[i]);
    }

    if (rdd->trans == JOIN && rdd->numdependencies == 2) {
        RDD* left = rdd->dependencies[0];
        RDD* right = rdd->dependencies[1];
        if (list_size(left->partitions) != list_size(right->partitions)) {
            return;
        }
    }

    if (rdd->trans == PARTITIONBY) {
        RDD* dep = rdd->dependencies[0];
        int num_parts = ((ExtendedRDD*)rdd)->numpartitions;
        List** outputs = malloc(sizeof(List*) * num_parts);
        for (int i = 0; i < num_parts; i++) {
            outputs[i] = list_init(10);
        }

        pthread_mutex_t* locks = malloc(sizeof(pthread_mutex_t) * num_parts);
        for (int j = 0; j < num_parts; j++) {
            pthread_mutex_init(&locks[j], NULL);
        }

        int np = list_size(dep->partitions);
        for (int i = 0; i < np; i++) {
            List* in_part = list_get_elem(dep->partitions, i);
            PartitionByTaskArg* arg = malloc(sizeof(PartitionByTaskArg));
            arg->header.rdd = rdd;
            arg->header.pnum = i;
            arg->input = in_part;
            arg->outputs = outputs;
            arg->num_outputs = num_parts;
            arg->partitioner = (Partitioner)rdd->fn;
            arg->ctx = rdd->ctx;
            arg->locks = locks;
            thread_pool_submit(process_partitionby_task, arg);
        }


        for (int j = 0; j < num_parts; j++) {
            pthread_mutex_destroy(&locks[j]);
        }
        free(locks);

        List* new_parts = list_init(num_parts);
        for (int i = 0; i < num_parts; i++) {
            list_add_elem(new_parts, outputs[i]);
        }
        free(outputs);
        rdd->partitions = new_parts;

        for (int i = 0; i < num_parts; i++) {
            TaskMetric* metric = malloc(sizeof(TaskMetric));
            clock_gettime(CLOCK_MONOTONIC, &metric->created);
            metric->rdd = rdd;
            metric->pnum = i;
            metric->scheduled = metric->created;  // Simplified for this case
            metric->duration = 0;  // Would be actual duration in real implementation
            
            pthread_mutex_lock(&metric_lock);
            list_add_elem(metric_queue, metric);
            pthread_cond_signal(&metric_cond);
            pthread_mutex_unlock(&metric_lock);
        }
        return;
    }

    if (!rdd->partitions) {
        int num_partitions = 1;
        if (rdd->trans == PARTITIONBY) {
            num_partitions = rdd->numpartitions;
        } else if (rdd->numdependencies > 0) {
            num_partitions = list_size(rdd->dependencies[0]->partitions);
        }

        rdd->partitions = list_init(num_partitions);
        for (int i = 0; i < num_partitions; i++) {
            list_add_elem(rdd->partitions, NULL);
        }
    }

    pthread_mutex_lock(&thread_pool->active_lock);
    thread_pool->active_tasks = list_size(rdd->partitions);
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
        pthread_cond_signal(&thread_pool->queue_cond);
        pthread_mutex_unlock(&thread_pool->queue_lock);
    }

    pthread_mutex_lock(&thread_pool->active_lock);
    while (thread_pool->active_tasks > 0) {
        pthread_cond_wait(&thread_pool->active_cond, &thread_pool->active_lock);
    }
    pthread_mutex_unlock(&thread_pool->active_lock);
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
                if (rdd->fn == (void*)GetLines ) {
                    printf("\n");
                }

                if (is_filter) {
                    printf("\n");
                }

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
    ExtendedRDD* rdd = malloc(sizeof(ExtendedRDD));
    rdd->base.trans = PARTITIONBY;
    rdd->base.fn = fn;
    rdd->base.ctx = ctx;
    rdd->base.numdependencies = 1;
    rdd->base.dependencies[0] = dep;
    rdd->base.partitions = NULL;
    rdd->numpartitions = numpartitions;
    return (RDD*) rdd;
}


RDD* join(RDD* dep1, RDD* dep2, Joiner fn, void* ctx) {
    /* DEBUG("Creating JOIN RDD from %p and %p", dep1, dep2); */
    RDD* rdd = create_rdd(2, JOIN, fn, dep1, dep2);
    rdd->ctx = ctx;
    return rdd;
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