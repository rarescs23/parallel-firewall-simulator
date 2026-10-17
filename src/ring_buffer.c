// SPDX-License-Identifier: BSD-3-Clause

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	if (ring == NULL || cap == 0)
		return -EINVAL;

	ring->data = malloc(cap);
	if (ring->data == NULL)
		return -ENOMEM;

	ring->cap = cap;
	ring->write_pos = ring->read_pos = 0;
	ring->len = 0;
	ring->stop = 0;
	ring->deq_count = 0;

	pthread_mutex_init(&ring->mutex, NULL);
	pthread_cond_init(&ring->not_empty, NULL);
	pthread_cond_init(&ring->not_full, NULL);

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* o cerere mai mare decat bufferul ar astepta la nesfarsit */
	if (ring == NULL || data == NULL || size == 0 || size > ring->cap)
		return -EINVAL;

	pthread_mutex_lock(&ring->mutex);

	/* buffer plin: producatorul asteapta pana cand consumatorii fac loc */
	while (ring->len + size > ring->cap && !ring->stop)
		pthread_cond_wait(&ring->not_full, &ring->mutex);

	if (ring->stop) {
		pthread_mutex_unlock(&ring->mutex);
		return -EPIPE;
	}

	/* copiere cu wrap-around (doua segmente daca depaseste capatul) */
	size_t first = ring->cap - ring->write_pos;

	if (first > size)
		first = size;
	memcpy(ring->data + ring->write_pos, data, first);
	memcpy(ring->data, (char *)data + first, size - first);

	ring->write_pos = (ring->write_pos + size) % ring->cap;
	ring->len += size;

	/* anunta un consumator ca exista date */
	pthread_cond_signal(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	return ring_buffer_dequeue_seq(ring, data, size, NULL);
}

ssize_t ring_buffer_dequeue_seq(so_ring_buffer_t *ring, void *data, size_t size, unsigned long *seq)
{
	if (ring == NULL || data == NULL || size == 0 || size > ring->cap)
		return -EINVAL;

	pthread_mutex_lock(&ring->mutex);

	/* buffer fara suficiente date: consumatorii asteapta */
	while (ring->len < size && !ring->stop)
		pthread_cond_wait(&ring->not_empty, &ring->mutex);

	/* producatorul s-a oprit si nu mai sunt date complete: consumatorul poate iesi */
	if (ring->len < size) {
		pthread_mutex_unlock(&ring->mutex);
		return -1;
	}

	size_t first = ring->cap - ring->read_pos;

	if (first > size)
		first = size;
	memcpy(data, ring->data + ring->read_pos, first);
	memcpy((char *)data + first, ring->data, size - first);

	ring->read_pos = (ring->read_pos + size) % ring->cap;
	ring->len -= size;
	if (seq != NULL)
		*seq = ring->deq_count;
	ring->deq_count++;

	/* anunta producatorul ca s-a eliberat loc */
	pthread_cond_signal(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	if (ring == NULL)
		return;

	free(ring->data);
	ring->data = NULL;
	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->not_full);
	pthread_cond_destroy(&ring->not_empty);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	pthread_mutex_lock(&ring->mutex);
	ring->stop = 1;
	/* trezeste toti consumatorii (si eventual un producator blocat) */
	pthread_cond_broadcast(&ring->not_empty);
	pthread_cond_broadcast(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);
}
