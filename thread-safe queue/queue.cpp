#include "queue.h"
#include <cstring>   // memcpy
#include <cstdlib>   // malloc, free
#include <mutex>

// ��ƿ: value ���� ����
char* deep_copy_value(const char* value, int size) {
    if (!value || size <= 0) return nullptr;
    char* copy = (char*)malloc(size);
    memcpy(copy, value, size);
    return copy;
}

Queue* init(void) {
    Queue* q = new Queue;
    q->head = nullptr;
    q->tail = nullptr;
    return q;
}

void release(Queue* queue) {
    {
    std::lock_guard<std::mutex> lock(queue->mtx);
    Node* curr = queue->head;
    while (curr) {
        Node* next = curr->next;
        if (curr->item.value) free(curr->item.value);
        delete curr;
        curr = next;
    }
    queue->head = queue->tail = nullptr;
   }
    delete queue;
}

Reply enqueue(Queue* queue, Item item) {
    Reply reply;
    reply.success = false;

    std::lock_guard<std::mutex> lock(queue->mtx);

    // �̹� �����ϴ� key�� value�� �����
    Node* curr = queue->head;
    while (curr) {
        if (curr->item.key == item.key) {
            // ���� value �޸� ����
            if (curr->item.value) free(curr->item.value);
            // ���ο� value ���� ����
            curr->item.value = deep_copy_value(item.value, item.value_size);
            curr->item.value_size = item.value_size;

            reply.success = true;
            reply.item.key = item.key;
            reply.item.value = deep_copy_value(curr->item.value, item.value_size); // ���� ����� ��ȯ
            reply.item.value_size = item.value_size;
            return reply;
        }
        curr = curr->next;
    }

    // ���ο� ��� ����
    Node* new_node = new Node;
    new_node->item.key = item.key;
    new_node->item.value = deep_copy_value(item.value, item.value_size);
    new_node->item.value_size = item.value_size;
    new_node->next = nullptr;

    // ���� ��ġ Ž�� (�켱���� ť: key ��������)
    if (!queue->head || item.key < queue->head->item.key) {
        new_node->next = queue->head;
        queue->head = new_node;
        if (!queue->tail) queue->tail = new_node;
    }
    else {
        Node* prev = queue->head;
        while (prev->next && prev->next->item.key < item.key) {
            prev = prev->next;
        }
        new_node->next = prev->next;
        prev->next = new_node;
        if (!new_node->next) queue->tail = new_node;
    }

    reply.success = true;
    reply.item.key = item.key;
    reply.item.value = deep_copy_value(item.value, item.value_size); // ���� ����� ��ȯ
    reply.item.value_size = item.value_size;
    return reply;
}

Reply dequeue(Queue* queue) {
    Reply reply;
    reply.success = false;

    std::lock_guard<std::mutex> lock(queue->mtx);
    if (!queue->head) return reply;

    Node* temp = queue->head;
    queue->head = temp->next;
    if (!queue->head) queue->tail = nullptr;

    reply.success = true;
    reply.item.key = temp->item.key;
    reply.item.value_size = temp->item.value_size;
    reply.item.value = deep_copy_value(temp->item.value, temp->item.value_size);

    if (temp->item.value) free(temp->item.value);
    delete temp;

    return reply;
}

Queue* range(Queue* queue, Key start, Key end) {
    Queue* result = init();
    std::lock_guard<std::mutex> lock(queue->mtx);

    Node* curr = queue->head;
    while (curr) {
        if (curr->item.key >= start && curr->item.key <= end) {
            Item copy;
            copy.key = curr->item.key;
            copy.value_size = curr->item.value_size;
            copy.value = deep_copy_value(curr->item.value, copy.value_size);
            enqueue(result, copy);
        }
        curr = curr->next;
    }
    return result;
}
