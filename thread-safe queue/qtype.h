#pragma once
#include <mutex>

typedef int Key;

typedef struct {
    Key key;
    char* value;
    int value_size;
} Item;

typedef struct node_t {
    Item item;
    struct node_t* next;
} Node;

typedef struct {
    Node* head;
    Node* tail;
    std::mutex mtx;
} Queue;

typedef struct {
    bool success;
    Item item;
} Reply;
