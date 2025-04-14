#ifndef __MINISPARK_H__
#define __MINISPARK_H__

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <time.h>

#define MAXDEPS 2
#define TIME_DIFF_MICROS(start, end) \
  (((end.tv_sec - start.tv_sec) * 1000000L) + ((end.tv_nsec - start.tv_nsec) / 1000L))

typedef struct RDD RDD;
typedef struct List List;

typedef struct TaskMetric {
    struct timespec created;
    struct timespec scheduled;
    size_t duration;
    RDD* rdd;
    int pnum;
} TaskMetric;

typedef struct Task {
    RDD* rdd;
    int pnum;
    TaskMetric* metric;
} Task;

typedef void* (*Mapper)(void* arg);
typedef int (*Filter)(void* arg, void* ctx);
typedef void* (*Joiner)(void* arg1, void* arg2, void* ctx);
typedef unsigned long (*Partitioner)(void* arg, int numpartitions, void* ctx);
typedef void (*Printer)(void* arg);

typedef enum {
  MAP,
  FILTER,
  JOIN,
  PARTITIONBY,
  FILE_BACKED
} Transform;

struct RDD {
  Transform trans;
  void* (*fn)(void*, void*, void*);
  void* ctx;
  List* partitions;
  
  RDD* dependencies[MAXDEPS];
  int numdependencies;
  int numpartitions;
  int file_processing_mode;
  char* filename;
};

struct List {
  void** elements;
  int size;
  int capacity;
};

List* list_init(int initial_capacity);
void list_add_elem(List* l, void* elem);
void* list_get_elem(List* l, int index);
void* list_remove_first(List* l);
void list_set_elem(List* l, int index, void* elem);
int list_size(List* l);
void list_free(List* l);

void MS_Run();
void MS_TearDown();
void execute(RDD* rdd);

int count(RDD* rdd);
void print(RDD* rdd, Printer p);

RDD* map(RDD* rdd, Mapper fn);
RDD* filter(RDD* rdd, Filter fn, void* ctx);
RDD* join(RDD* rdd1, RDD* rdd2, Joiner fn, void* ctx);
RDD* partitionBy(RDD* rdd, Partitioner fn, int numpartitions, void* ctx);
RDD* RDDFromFiles(char* filenames[], int numfiles);

void print_formatted_metric(TaskMetric* metric, FILE* fp);

#endif // __MINISPARK_H__