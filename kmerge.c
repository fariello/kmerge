#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#define DEFAULT_NORMAL_CAPACITY (16 * 1024)
#define DEFAULT_JUMBO_THRESHOLD (100 * 1024 * 1024)

typedef struct {
    FILE *fp;
    char *normal_buf;
    char *jumbo_buf;
    char *current_line;  // pointer matching active layer organically
    size_t line_len;
    size_t capacity;
    unsigned int stream_id; // origin file index strictly bounding topological stability constraints natively
    bool is_jumbo;
    bool eof;
    char *filename;
} kStreamState;

typedef struct {
    int size;
    int *nodes;
    kStreamState *streams;
} MinHeap;

static inline int compare_streams(kStreamState *a, kStreamState *b) {
    if (a->eof && b->eof) return 0;
    if (a->eof) return 1;
    if (b->eof) return -1;
    
    size_t min_len = (a->line_len < b->line_len) ? a->line_len : b->line_len;
    int cmp = memcmp(a->current_line, b->current_line, min_len);
    if (cmp != 0) return cmp;
    
    if (a->line_len < b->line_len) return -1;
    if (a->line_len > b->line_len) return 1;
    
    if (a->stream_id < b->stream_id) return -1;
    return 1;
}

static inline void heapify_down(MinHeap *h, int idx) {
    int smallest = idx;
    int left = 2 * idx;
    int right = 2 * idx + 1;
    
    if (left <= h->size && compare_streams(&h->streams[h->nodes[left]], &h->streams[h->nodes[smallest]]) < 0)
        smallest = left;
    if (right <= h->size && compare_streams(&h->streams[h->nodes[right]], &h->streams[h->nodes[smallest]]) < 0)
        smallest = right;
        
    if (smallest != idx) {
        int temp = h->nodes[idx];
        h->nodes[idx] = h->nodes[smallest];
        h->nodes[smallest] = temp;
        // Optimization natively avoiding recursion footprint for raw loops.
        heapify_down(h, smallest); 
    }
}

static inline bool read_next_line(kStreamState *stream, size_t jumbo_threshold) {
    if (stream->is_jumbo) {
        free(stream->jumbo_buf);
        stream->jumbo_buf = NULL;
        stream->is_jumbo = false;
        stream->capacity = DEFAULT_NORMAL_CAPACITY;
        stream->current_line = stream->normal_buf;
    }

    size_t pos = 0;
    while(1) {
        int c = getc_unlocked(stream->fp);
        if (c == EOF) {
            if (pos == 0) {
                stream->eof = true;
                return false;
            }
            break;
        }

        if (pos >= stream->capacity) {
            if (pos >= jumbo_threshold) {
                fprintf(stderr, "\n[kmerge] FATAL: Line inside stream '%s' exceeds active jumbo threshold bounds (%zu bytes) natively.\n", stream->filename, jumbo_threshold);
                exit(EXIT_FAILURE);
            }
            
            size_t new_cap = stream->capacity * 2;
            if (new_cap > jumbo_threshold) new_cap = jumbo_threshold + 1;
            
            if (!stream->is_jumbo) {
                stream->jumbo_buf = malloc(new_cap);
                if (!stream->jumbo_buf) { perror("malloc jumbo bounds"); exit(EXIT_FAILURE); }
                memcpy(stream->jumbo_buf, stream->normal_buf, pos);
                stream->current_line = stream->jumbo_buf;
                stream->is_jumbo = true;
            } else {
                stream->jumbo_buf = realloc(stream->jumbo_buf, new_cap);
                if (!stream->jumbo_buf) { perror("realloc jumbo bounds"); exit(EXIT_FAILURE); }
                stream->current_line = stream->jumbo_buf;
            }
            stream->capacity = new_cap;
        }

        stream->current_line[pos++] = (char)c;
        if (c == '\n') break;
    }
    
    stream->line_len = pos;
    return true;
}


int main(int argc, char **argv) {
    char *output_file = NULL;
    size_t normal_cap = DEFAULT_NORMAL_CAPACITY;
    size_t jumbo_threshold = DEFAULT_JUMBO_THRESHOLD;
    bool progress_mode = false;
    
    static struct option long_options[] = {
        {"output", required_argument, 0, 'o'},
        {"expected-length", required_argument, 0, 'e'},
        {"jumbo-threshold", required_argument, 0, 'j'},
        {"progress", no_argument, 0, 'p'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "o:e:j:p", long_options, NULL)) != -1) {
        switch (opt) {
            case 'o': output_file = optarg; break;
            case 'e': normal_cap = (size_t)atol(optarg); break;
            case 'j': jumbo_threshold = (size_t)atol(optarg); break;
            case 'p': progress_mode = true; break;
            default:
                fprintf(stderr, "Usage: %s [OPTIONS] FILE1 FILE2 ...\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
    
    int num_files = argc - optind;
    if (num_files < 1) {
        fprintf(stderr, "[kmerge] FATAL: Requires bounds mapping at least 1 file target explicitly.\n");
        exit(EXIT_FAILURE);
    }
    
    kStreamState *streams = malloc(sizeof(kStreamState) * num_files);
    int active_streams = 0;
    
    for (int i = 0; i < num_files; i++) {
        char *fname = argv[optind + i];
        FILE *f = fopen(fname, "r");
        if (!f) {
            perror(fname);
            exit(EXIT_FAILURE);
        }
        
        // Push rigid unbuffered optimization rules dynamically over sequential targets
        setvbuf(f, NULL, _IOFBF, 2 * 1024 * 1024);
        
        streams[active_streams].fp = f;
        streams[active_streams].normal_buf = malloc(normal_cap);
        streams[active_streams].jumbo_buf = NULL;
        streams[active_streams].current_line = streams[active_streams].normal_buf;
        streams[active_streams].capacity = normal_cap;
        streams[active_streams].stream_id = active_streams;
        streams[active_streams].is_jumbo = false;
        streams[active_streams].eof = false;
        streams[active_streams].filename = fname;
        
        // Prime explicit tracking sequence cleanly over bounds.
        if (read_next_line(&streams[active_streams], jumbo_threshold)) {
            active_streams++;
        } else {
            fclose(f);
            free(streams[active_streams].normal_buf);
        }
    }
    
    if (active_streams == 0) {
        fprintf(stderr, "[kmerge] INFO: All explicitly mapped files are completely mathematically empty.\n");
        return EXIT_SUCCESS;
    }
    
    FILE *out = stdout;
    if (output_file) {
        out = fopen(output_file, "w");
        if (!out) {
            perror(output_file);
            exit(EXIT_FAILURE);
        }
    }
    // Set Output explicitly bounding rigid 2MB thresholds naturally bypassing flush boundaries.
    setvbuf(out, NULL, _IOFBF, 2 * 1024 * 1024);
    
    MinHeap heap;
    heap.size = active_streams;
    heap.streams = streams;
    // 1-based indexing explicitly mapped for boundary mathematics.
    heap.nodes = malloc(sizeof(int) * (active_streams + 1));
    for (int i = 0; i < active_streams; i++) {
        heap.nodes[i + 1] = i; 
    }
    
    // Explicit topological Heap Initialization O(K) footprint cleanly.
    for (int i = heap.size / 2; i >= 1; i--) {
        heapify_down(&heap, i);
    }
    
    unsigned long long total_emitted = 0;
    time_t last_progress_time = time(NULL);
    
    while (heap.size > 0) {
        int winner = heap.nodes[1];
        kStreamState *w_stream = &streams[winner];
        
        fwrite(w_stream->current_line, 1, w_stream->line_len, out);
        total_emitted++;
        
        if (progress_mode) {
            time_t current = time(NULL);
            if (current - last_progress_time >= 5) {
                fprintf(stderr, "[kmerge stats] Emitted sequentially over limit: %llu rows natively...\n", total_emitted);
                last_progress_time = current;
            }
        }
        
        if (read_next_line(w_stream, jumbo_threshold)) {
            heapify_down(&heap, 1);
        } else {
            heap.nodes[1] = heap.nodes[heap.size];
            heap.size--;
            if (heap.size > 0) {
                heapify_down(&heap, 1);
            }
        }
    }
    
    if (out != stdout) {
        fclose(out);
    } else {
        fflush(stdout); 
    }
    
    for (int i = 0; i < active_streams; i++) {
        fclose(streams[i].fp);
        free(streams[i].normal_buf);
        if (streams[i].jumbo_buf) free(streams[i].jumbo_buf);
    }
    free(streams);
    free(heap.nodes);
    
    if (progress_mode) {
        fprintf(stderr, "[kmerge stats] Merge fully finalized natively: %llu structures perfectly mapped.\n", total_emitted);
    }
    
    return EXIT_SUCCESS;
}
