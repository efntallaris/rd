/**
 * @file        vector.h
 * @author      Michal Pajtinka
 *
 * @brief Interface for creating vector.
 *
 * This file declares functions, types and macros for creating containers
 * for data of any type (similar to vectors used in C++).
 * 
 * Creating and destroying Vectors:
 *      vector_new
 *      vector_delete
 *
 * Finding important indices:
 *      vector_begin
 *      vector_end
 *
 * Info about capacity:
 *      vector_size
 *      vector_resize
 *      vector_capacity
 *      vector_empty
 *
 * Accessing elements:
 *      vector_front
 *      vector_back
 *      vector_at
 *
 * Modifying Vector content:
 *      vector_push_back
 *      vector_pop_back
 *      vector_insert
 *      vector_erase
 *      vector_swap
 *      vector_clear
 */

#ifndef VECTOR_H
#define VECTOR_H

#include <stdio.h>
#include <stdbool.h>

#define INIT_SIZE       2               /** initial vector capacity */
#define GROWTH_RATE     2               /** the vector capacity growth speed */
#define NPOS            ((size_t)-1)    /** maximum value of size_t */

/**
 * @brief Definition of type Vector
 *
 * Vector is an opaque type. To work with it, use provided functions, but do
 * not try to access its members directly.
 */
typedef struct vector *Vector;


/**
 * @brief Initialize new vector with some default values.
 *
 * Memory for new Vector is allocated. When no longer needed,
 * the memory should be freed by calling vector_delete() function
 * to avoid memory leaks.
 *
 * @return      pointer to newly created vector or
 *              @c NULL if the vector is not created
 */
Vector vector_new();


/**
 * @brief Erase all elements and free all alocated memory.
 *
 * This function deallocates all memory allocated for Vector by vector_new()
 * (BUT NOT THE STORED POINTERS - THOSE DON`T NEED TO BE ALLOCATED AT ALL,
 * AND IF THEY ARE, THEY HAVE TO BE FREED MANUALLY). However, @p vec will
 * be still pointing to it`s old location, so it is recommended to set it to
 * @c NULL later after calling this function to avoid subsequent errors.
 *
 * @param vec   vector to be erased
 */
void vector_delete(Vector vec);


/**
 * @brief Get index of the first element
 *
 * @param vec   vector to be processed
 * @return      @c NPOS if vector is empty or if @p vec == @c NULL,
 *              otherwise @c 0 (index of first element)
 */
size_t vector_begin(Vector vec);


/**
 * @brief Get index of the last element
 *
 * @param vec   vector to be processed
 * @return      @c NPOS if vector is empty or if @p vec == @c NULL,
 *              otherwise @c 0 (index of first element) 
 */
size_t vector_end(Vector vec);


/**
 * @brief Get number of elements in vector.
 *
 * @param vec   vector to be processed
 * @return      number of vecetor elements or
 *              @c NPOS if @p vec is @c NULL
 */
size_t vector_size(Vector vec);


/**
 * @brief Change the capacity of vector
 *
 * This function allocates memory for @p num of elements.
 * If @p num is:
 * a) bigger than current capacity:
 *      - data array is realloced so that @p num elements fit in
 * b) smaller than current capacity:
 *      - capacity is reduced to @p num elements
 *      - if @p num is also smaller then current number of elements,
 *        content of vector is reduced to its first @p num elements, removing
 *        elements behind them (those are not free`d, if were allocated before)
 * c) special value NPOS:
 *      - size is adjusted automatically
 *      - when called, function checks, whether there is space for another
 *        element:
 *              - if so, it does nothing, just returns @c true
 *              - if there is no more space, it increases size to
 *                GROWTH_RATE times the current capacity
 *
 * Realloc() function is used to change the capacity, when passing @c zero
 * as argument, its behaviour is not defined by standard!!!
 *
 * @param vec   vector to be modified
 * @param num   total number of elements that can be stored in the vector
 * @return      true, if reallocation succeed or if there is enough space
 */
bool vector_resize(Vector vec, size_t num);


/**
 * @brief Returns current vector capacity
 *
 * @param vec   vector to be processed
 * @return      current vector capacity or @c NPOS if @p vec is @c NULL
 */
size_t vector_capacity(Vector vec);


/**
 * @brief Find whether vector is empty.
 *
 * @param vec   vector to be processed
 * @return      @c true if empty or @p vec is @c NULL, @c false otherwise
 */
bool vector_empty(Vector vec);


/**
 * @brief Get first element of the given vector.
 *
 * @param vec   vector to be processed
 * @return      @c NULL if the vector is empty,
 *              otherwise pointer to first element
 */
void *vector_front(Vector vec);


/**
 * @brief Get last element of the given vector.
 *
 * @param vec   vector to be processed
 * @return      @c NULL if the vector is empty,
 *              otherwise pointer to the last element
 */
void *vector_back(Vector vec);


/**
 * @brief Get element from given position of the vector
 *
 * Initial element of a sequence is assigned the index 0
 *
 * @param vec   vector to be processed
 * @param pos   position of element
 * @return      @c NULL if the vector is empty,
 *              otherwise pointer to element on given position
 */
void *vector_at(Vector vec, size_t pos);


/**
 * @brief Insert element at the end of the vector.
 *
 * In case of memory allocaton fail, this function only returns false,
 * inner attributes of vector structure remain untouched until allocation
 * success.
 *
 * @param vec   vector to be processed
 * @param data  data to be inserted
 *
 * @return      @c true if successful, @c false otherwise
 */
bool vector_push_back(Vector vec, void *data);


/**
 * @brief Remove last element of the vector.
 * 
 * THIS ONLY REMOVES POINTER TO THE LAST ELEMENT OF THE VECTOR,
 * MAKE SURE YOU FREE THE MEMORY THE LAST POINTER IS POINTING TO (IF IT WAS
 * ALLOCATED EARLIER) BEFORE CALLING THIS FUNCTION
 *
 * @param vec   vector to be processed
 * @return      @c true if successful, @c false otherwise
 */
bool vector_pop_back(Vector vec);


/**
 * @brief Insert element on given position
 *
 * Vector stores its data in array, thus inserting element on position other
 * than last causes all subsequent elements to be moved one position forward.
 *
 * @param vec   vector to be processed
 * @param pos   position of new element in the vector
 * @param data  pointer to data to be stored in vector
 * @return      @c true, if element was inserted, @c false otherwise
 */
bool vector_insert(Vector vec, size_t pos, void *data);


/**
 * @brief Remove element on given position
 *
 * Vector stores its data in array, thus deleting element from position other
 * than last causes all subsequent elements to be moved one position back.
 *
 * @param vec   vector to be processed
 * @param pos   position of new element in the vector
 * @return      @c true, if element was erased, @c false otherwise
 */
bool vector_erase(Vector vec, size_t pos);


/**
 * @brief Swaps content of 2 vector variables
 *
 * @param first,second  pointers to vector variables to be swapped
 */
bool vector_swap(Vector first, Vector second);


/**
 * @brief Removes all elements from vector
 *
 * This function removes all elements from vector, leaving @p vec empty and
 * setting vec size to 0. Memory allocated for vector data remains the same.
 *
 * @param vec   vector to be processed
 */
void vector_clear(Vector vec);

#endif /* end of include guard: VECTOR_H */
