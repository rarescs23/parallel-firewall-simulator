/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_RINGBUFFER_H__
#define __SO_RINGBUFFER_H__

#include <sys/types.h>
#include <string.h>
#include <pthread.h>

typedef struct so_ring_buffer_t
{
	char *data;
	size_t read_pos;
	size_t write_pos;

	size_t len;
	size_t cap;
	int stop;
	unsigned long deq_count; /* cate pachete au iesit pana acum (numar de ordine FIFO) */

	pthread_mutex_t mutex;
	pthread_cond_t not_full;
	pthread_cond_t not_empty;
} so_ring_buffer_t;

int ring_buffer_init(so_ring_buffer_t *rb, size_t cap);
ssize_t ring_buffer_enqueue(so_ring_buffer_t *rb, void *data, size_t size);
ssize_t ring_buffer_dequeue(so_ring_buffer_t *rb, void *data, size_t size);
/*
 * Ca ring_buffer_dequeue, dar returneaza in *seq numarul de ordine al elementului scos
 * (0, 1, 2, ... in ordinea in care a fost scris de producator). Atribuirea numarului se face
 * sub acelasi lock ca scoaterea, deci este corecta si in prezenta mai multor consumatori.
 */
ssize_t ring_buffer_dequeue_seq(so_ring_buffer_t *rb, void *data, size_t size, unsigned long *seq);
void ring_buffer_destroy(so_ring_buffer_t *rb);
void ring_buffer_stop(so_ring_buffer_t *rb);

#endif /* __SO_RINGBUFFER_H__ */
