#ifndef WORK_ITEM_H
#define WORK_ITEM_H

#include "transport.hpp"

/**
 * work_item_t
 * Unit of work placed on the
 * work queue of a range server
 *
 * Each work_item_t is a node
 * in a linked list
 *
 * address is the source of the message
 *
 */
typedef struct work_item {
    virtual ~work_item() {}

	work_item *next;
	work_item *prev;
	TransportMessage *message;

} work_item_t;

#endif
