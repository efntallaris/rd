/**
 * @file        vector.c
 * @author      Michal Pajtinka
 *
 * @brief Functions for creating and accessing vecor.
 *
 * This file declares vector structure and functions designed
 * to work with this structure.
 */

#include "vector.h"
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

struct vector {
        void **data_array;      /** array of data pointers */
        size_t size;            /** actual number of elements in array */
        size_t capacity;        /** how much elements can be stored in array */
};


Vector vector_new()
{
        Vector vec = malloc(sizeof(struct vector));
        if (vec != NULL) {
                vec->data_array = NULL;
                vec->size = 0;
                vec->capacity = 0;
        }
        return vec;
}


void vector_delete(Vector vec)
{
        if (vec != NULL) {
                if (vec->data_array != NULL) {
                        free(vec->data_array);
                }
                free(vec);
        }
}


size_t vector_begin(Vector vec)
{
        return (vec != NULL && vec->size > 0) ? 0 : NPOS;
}


size_t vector_end(Vector vec)
{
        return (vec != NULL && vec->size > 0) ? (vec->size - 1) : NPOS;
}


size_t vector_size(Vector vec)
{
        return (vec != NULL) ? vec->size : NPOS;
}


bool vector_resize(Vector vec, size_t num)
{
        if (vec == NULL) {
                return false;
        }

        size_t new_size = 0;
        if (num != NPOS) {
                if (vec->capacity > num) {
                        vec->capacity = num;
                        /* in case of schrinking some elements
                           may not fit in anymore */
                        if (vec->size > num) {
                                vec->size = num;
                        }
                        return true;
                }
                new_size = num;
        } else if (vec->size == vec->capacity) {
                /* set to "automatic" and no more space */
                new_size = (vec->capacity == 0) ? INIT_SIZE :
                                                (vec->capacity * GROWTH_RATE);
        } else {
                /* set to "automatic" and enough space */
                return true;
        }

        void *tmp_ptr = realloc(vec->data_array, new_size * sizeof(void*));

        /* if all went fine, update structure members */
        if (tmp_ptr == NULL) {
                return false;
        }
        vec->data_array = tmp_ptr;
        vec->capacity = new_size;

        /* it is not really necessary to initialize additionally
           allocated memory to null pointers now, as all functions
           operate only over elements up to data_array[vec->size - 1],
           but it is nice to do it to prevent surprises in case of
           further changes */
        void* new_area_begin = (void*)((ptrdiff_t)vec->data_array
                                                + (vec->size * sizeof(void*)));
        size_t new_area_size = (new_size - vec->size) * sizeof(void*);
        memset(new_area_begin, (ptrdiff_t)NULL, new_area_size);

        return true;
}


size_t vector_capacity(Vector vec)
{
        return (vec == NULL) ? NPOS : vec->capacity;
}


bool vector_empty(Vector vec)
{
        return (vec != NULL && vec->size > 0) ? false : true;
}


void *vector_front(Vector vec)
{
        return (vec != NULL && vec->size > 0) ? vec->data_array[0] : NULL;
}


void *vector_back(Vector vec)
{
        return (vec != NULL && vec->size > 0) ? vec->data_array[vec->size-1] : NULL;
}


void *vector_at(Vector vec, size_t pos)
{
        return (vec != NULL && vec->size > 0 && pos < vec->size) ? vec->data_array[pos] : NULL;
}


bool vector_push_back(Vector vec, void *data)
{
        return vector_insert(vec, vector_end(vec) + 1, data);
}


bool vector_pop_back(Vector vec)
{
        return vector_erase(vec, vector_end(vec));
}


bool vector_insert(Vector vec, size_t pos, void *data)
{
        if (vec == NULL || pos == NPOS || pos > vec->size ||
                                !vector_resize(vec, NPOS)) {
                return false;
        }

        for (size_t i = vec->size; i > pos; i--) {
                vec->data_array[i] = vec->data_array[i-1];
        }

        vec->data_array[pos] = data;
        vec->size++;

        return true;
}


bool vector_erase(Vector vec, size_t pos)
{
        if (vec == NULL || pos == NPOS || pos >= vec->size) {
                return false;
        }

        for (size_t i = pos; i < vec->size; i++) {
                vec->data_array[i] = vec->data_array[i+1];
        }

        vec->size--;

        return true;
}


bool vector_swap(Vector first, Vector second)
{
        if (first == NULL || second == NULL) {
                return false;
        }

        void **data = first->data_array;
        size_t size = first->size;
        size_t capacity = first->capacity;

        first->data_array = second->data_array;
        first->size = second->size;
        first->capacity = second->capacity;

        second->data_array = data;
        second->size = size;
        second->capacity = capacity;

        return true;
}


void vector_clear(Vector vec)
{
        if (vec == NULL) {
                return;
        }

        vec->size = 0;
}
