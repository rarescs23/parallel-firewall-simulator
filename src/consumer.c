// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

/* Dimensiunea buffer-ului local in care se aduna liniile de log inainte de write(). */
#define LOG_BATCH_SZ 4096
#define LOG_LINE_MAX 64

static void heap_init(min_heap_t *heap)
{
	heap->seq = NULL;
	heap->action = NULL;
	heap->hash = NULL;
	heap->timestamp = NULL;
	heap->size = 0;
	heap->capacity = 0;
}

static void heap_grow(min_heap_t *heap)
{
	size_t new_cap = heap->capacity ? heap->capacity * 2 : 64;

	heap->seq = realloc(heap->seq, new_cap * sizeof(*heap->seq));
	heap->action = realloc(heap->action, new_cap * sizeof(*heap->action));
	heap->hash = realloc(heap->hash, new_cap * sizeof(*heap->hash));
	heap->timestamp = realloc(heap->timestamp, new_cap * sizeof(*heap->timestamp));
	DIE(!heap->seq || !heap->action || !heap->hash || !heap->timestamp, "realloc heap");

	heap->capacity = new_cap;
}

static void heap_push(min_heap_t *heap, unsigned long seq, so_action_t action,
		      unsigned long hash, unsigned long timestamp)
{
	if (heap->size == heap->capacity)
		heap_grow(heap);

	size_t i = heap->size++;

	/* sift-up: mentine radacina = pachetul cu cel mai mic numar de ordine */
	while (i > 0) {
		size_t parent = (i - 1) / 2;

		if (heap->seq[parent] <= seq)
			break;

		heap->seq[i] = heap->seq[parent];
		heap->action[i] = heap->action[parent];
		heap->hash[i] = heap->hash[parent];
		heap->timestamp[i] = heap->timestamp[parent];
		i = parent;
	}

	heap->seq[i] = seq;
	heap->action[i] = action;
	heap->hash[i] = hash;
	heap->timestamp[i] = timestamp;
}

/* Extrage radacina (heap-ul trebuie sa nu fie gol). */
static void heap_pop(min_heap_t *heap, so_action_t *action, unsigned long *hash, unsigned long *timestamp)
{
	*action = heap->action[0];
	*hash = heap->hash[0];
	*timestamp = heap->timestamp[0];

	heap->size--;
	if (heap->size == 0)
		return;

	unsigned long last_seq = heap->seq[heap->size];
	so_action_t last_action = heap->action[heap->size];
	unsigned long last_hash = heap->hash[heap->size];
	unsigned long last_timestamp = heap->timestamp[heap->size];
	size_t i = 0;

	/* sift-down */
	while (2 * i + 1 < heap->size) {
		size_t smallest = 2 * i + 1;

		if (smallest + 1 < heap->size && heap->seq[smallest + 1] < heap->seq[smallest])
			smallest++;

		if (last_seq <= heap->seq[smallest])
			break;

		heap->seq[i] = heap->seq[smallest];
		heap->action[i] = heap->action[smallest];
		heap->hash[i] = heap->hash[smallest];
		heap->timestamp[i] = heap->timestamp[smallest];
		i = smallest;
	}

	heap->seq[i] = last_seq;
	heap->action[i] = last_action;
	heap->hash[i] = last_hash;
	heap->timestamp[i] = last_timestamp;
}

static void heap_destroy(min_heap_t *heap)
{
	free(heap->seq);
	free(heap->action);
	free(heap->hash);
	free(heap->timestamp);
}

static void write_all(int fd, const char *buf, size_t len)
{
	while (len > 0) {
		ssize_t rc = write(fd, buf, len);

		DIE(rc < 0, "write log");
		buf += rc;
		len -= rc;
	}
}

/*
 * Scrie in log pachetele consecutive care sunt gata (seq == next_write_seq). Se apeleaza cu
 * heap_mutex luat si il pastreaza luat la iesire.
 *
 * Doar un singur thread ("flusher") scrie la un moment dat. El scoate din heap un lot de linii
 * consecutive, ELIBEREAZA lock-ul cat timp face write() (ceilalti consumatori continua sa insereze
 * rezultate) si apoi reia. Un thread care gaseste deja un flusher activ doar insereaza si pleaca:
 * flusher-ul va prelua si elementele lui. Nimeni nu asteapta si nu face busy-wait, iar logul este
 * scris in ordine crescatoare in timpul procesarii, nu sortat la final.
 */
static void flush_ready(so_consumer_ctx_t *ctx)
{
	min_heap_t *heap = &ctx->heap;
	char batch[LOG_BATCH_SZ];

	if (ctx->flushing)
		return;

	ctx->flushing = 1;

	while (1) {
		size_t used = 0;

		while (heap->size > 0 && heap->seq[0] == ctx->next_write_seq &&
		       used + LOG_LINE_MAX <= sizeof(batch)) {
			so_action_t action;
			unsigned long hash, timestamp;

			heap_pop(heap, &action, &hash, &timestamp);
			ctx->next_write_seq++;
			used += snprintf(batch + used, sizeof(batch) - used, "%s %016lx %lu\n",
					 RES_TO_STR(action), hash, timestamp);
		}

		if (used == 0)
			break; /* nimic gata de scris; flushing se reseteaza sub acelasi lock */

		pthread_mutex_unlock(&ctx->heap_mutex);
		write_all(ctx->log_fd, batch, used);
		pthread_mutex_lock(&ctx->heap_mutex);
	}

	ctx->flushing = 0;
}

static void *consumer_thread(void *arg)
{
	so_consumer_ctx_t *ctx = arg;
	so_packet_t pkt;

	while (1) {
		unsigned long seq;
		ssize_t rc;

		/*
		 * Numarul de ordine este atribuit de ring buffer sub propriul lock, in ordinea FIFO
		 * a producatorului, deci reflecta exact ordinea de trimitere a pachetelor.
		 */
		rc = ring_buffer_dequeue_seq(ctx->producer_rb, &pkt, PKT_SZ, &seq);

		if (rc != PKT_SZ)
			break; /* buffer oprit si golit */

		/* procesarea (partea costisitoare) ruleaza in paralel, fara niciun lock */
		unsigned long hash = packet_hash(&pkt);
		unsigned long timestamp = pkt.hdr.timestamp;
		so_action_t action = process_packet(&pkt);

		pthread_mutex_lock(&ctx->heap_mutex);
		heap_push(&ctx->heap, seq, action, hash, timestamp);
		flush_ready(ctx);
		pthread_mutex_unlock(&ctx->heap_mutex);
	}

	/* ultimul consumator care termina elibereaza resursele partajate */
	pthread_mutex_lock(&ctx->heap_mutex);
	int last = (--ctx->active_consumers == 0);

	pthread_mutex_unlock(&ctx->heap_mutex);

	if (last) {
		close(ctx->log_fd);
		heap_destroy(&ctx->heap);
		pthread_mutex_destroy(&ctx->heap_mutex);
		free(ctx);
	}

	return NULL;
}

int create_consumers(pthread_t *tids, int num_consumers, struct so_ring_buffer_t *rb,
		     const char *out_filename)
{
	so_consumer_ctx_t *ctx = calloc(1, sizeof(*ctx));

	DIE(ctx == NULL, "calloc consumer ctx");

	ctx->log_fd = open(out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	DIE(ctx->log_fd < 0, "open output file");

	ctx->producer_rb = rb;
	ctx->active_consumers = num_consumers;
	heap_init(&ctx->heap);
	pthread_mutex_init(&ctx->heap_mutex, NULL);

	for (int i = 0; i < num_consumers; i++) {
		int rc = pthread_create(&tids[i], NULL, consumer_thread, ctx);

		DIE(rc != 0, "pthread_create");
	}

	return num_consumers;
}
