/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include <pthread.h>
#include <stddef.h>
#include "ring_buffer.h"
#include "packet.h"

/*
 * Min-heap de rezultate procesate, ordonat dupa numarul de ordine (seq) al pachetului.
 * Numarul de ordine este atribuit la scoaterea din ring buffer (ordinea producatorului),
 * iar timestamp-urile cresc strict in aceeasi ordine, deci ordinea dupa seq este si ordinea
 * dupa timestamp.
 */
typedef struct {
	unsigned long *seq;
	so_action_t *action;
	unsigned long *hash;
	unsigned long *timestamp;
	size_t size;
	size_t capacity;
} min_heap_t;

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;
	min_heap_t heap;

	/* Protejeaza heap-ul, contorul next_write_seq si scrierea in log. */
	pthread_mutex_t heap_mutex;
	unsigned long next_write_seq;
	int flushing; /* 1 daca un thread scrie deja loturi in log */

	int log_fd;
	int active_consumers;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids, int num_consumers, so_ring_buffer_t *rb, const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
