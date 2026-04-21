#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <locale.h>
#include <sys/stat.h>

static bool use_color = false;

#define K_PREFIX (use_color ? "[\033[36mkmerge\033[0m]" : "[kmerge]")
#define FATAL_PREFIX (use_color ? "\033[31mFATAL:\033[0m" : "FATAL:")
#define INFO_PREFIX (use_color ? "\033[33mINFO:\033[0m" : "INFO:")

#define DEFAULT_NORMAL_CAPACITY (16 * 1024)
#define DEFAULT_JUMBO_THRESHOLD (100 * 1024 * 1024)

typedef struct {
    FILE *fp;
    char *normal_buf;
    char *jumbo_buf;
    char *current_line;  // pointer matching active layer organically
    size_t line_len;
    size_t capacity;
    size_t original_normal_cap;
    unsigned int stream_id; // origin file index strictly bounding topological stability constraints natively
    bool is_jumbo;
    bool eof;
    char *filename;
    dev_t st_dev;
    ino_t st_ino;
} kStreamState;

static void format_bytes(double bytes, char *buf) {
    const char *units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    int i = 0;
    while (bytes >= 1024.0 && i < 5) {
        bytes /= 1024.0;
        i++;
    }
    sprintf(buf, "%.2f %s", bytes, units[i]);
}

static void format_commas(unsigned long long n, char *out) {
    char buf[64];
    int len = sprintf(buf, "%llu", n);
    int out_ptr = 0;
    for (int i = 0; i < len; i++) {
        if (i > 0 && (len - i) % 3 == 0) {
            out[out_ptr++] = ',';
        }
        out[out_ptr++] = buf[i];
    }
    out[out_ptr] = '\0';
}

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
    while (1) {
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
            idx = smallest;
        } else {
            break;
        }
    }
}

/* Returns true if the root is still the heap minimum, i.e. it still beats
 * both of its children. When this is the case after reading a new line from
 * the winning stream, we can skip heapify_down entirely. */
static inline bool root_still_wins(MinHeap *h) {
    /* With only 0 or 1 streams left there is nothing to compare against. */
    if (h->size <= 1) return true;
    int left  = 2;
    int right = 3;
    /* Check left child (always exists when size >= 2). */
    if (compare_streams(&h->streams[h->nodes[left]], &h->streams[h->nodes[1]]) < 0)
        return false;
    /* Check right child if it exists. */
    if (right <= h->size &&
        compare_streams(&h->streams[h->nodes[right]], &h->streams[h->nodes[1]]) < 0)
        return false;
    return true;
}

static inline bool read_next_line(kStreamState *stream, size_t jumbo_threshold) {
    if (stream->is_jumbo) {
        free(stream->jumbo_buf);
        stream->jumbo_buf = NULL;
        stream->is_jumbo = false;
        stream->capacity = stream->original_normal_cap;
        stream->current_line = stream->normal_buf;
    }

    size_t pos = 0;
    while(1) {
        int c = getc_unlocked(stream->fp);
        if (c == EOF) {
            if (ferror(stream->fp)) { perror("getc_unlocked"); exit(EXIT_FAILURE); }
            if (pos == 0) {
                stream->eof = true;
                return false;
            }
            break;
        }

        if (pos >= stream->capacity) {
            if (pos >= jumbo_threshold) {
                fprintf(stderr, "\n%s %s Line in file '%s' exceeds jumbo threshold (%zu bytes).\n", K_PREFIX, FATAL_PREFIX, stream->filename, jumbo_threshold);
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
    
    if (pos > 0 && stream->current_line[pos - 1] != '\n') {
        if (pos >= stream->capacity) {
            size_t new_cap = stream->capacity + 1;
            if (new_cap > jumbo_threshold) {
                fprintf(stderr, "\n%s %s Appending newline exceeds jumbo threshold.\n", K_PREFIX, FATAL_PREFIX);
                exit(EXIT_FAILURE);
            }
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
        stream->current_line[pos++] = '\n';
    }
    
    stream->line_len = pos;
    return true;
}

static void print_help(const char *prog_name) {
    printf("Usage: %s [OPTIONS] FILE1 FILE2 ...\n\n", prog_name);
    printf("A high-performance C-based K-way merge utility for line-based files.\n");
    printf("Files must be lexicographically sorted prior to merging.\n\n");
    printf("Options:\n");
    printf("  -o, --output <file>          Path to the output file (default: stdout)\n");
    printf("  -e, --expected-length <size> Expected initial capacity for normal lines in bytes (default: %d)\n", DEFAULT_NORMAL_CAPACITY);
    printf("  -j, --jumbo-threshold <size> Maximum line length threshold in bytes before failure (default: %d)\n", DEFAULT_JUMBO_THRESHOLD);
    printf("  -p, --progress               Enable periodic progress reporting to stderr\n");
    printf("  -f, --progress-interval <s > Specify progress reporting interval in seconds (default: 5)\n");
    printf("  -c, --color                  Enable ANSI colored terminal outputs\n");
    printf("  -F, --force                  Force overwrite of the output file if it already exists\n");
    printf("  -h, -?, --help               Display this detailed help message and exit\n");
    printf("\nExample:\n");
    printf("  %s -e 8192 -p -f 10 -o merged.csv chunk1.csv chunk2.csv chunk3.csv\n", prog_name);
}

int main(int argc, char **argv) {
    char *output_file = NULL;
    size_t normal_cap = DEFAULT_NORMAL_CAPACITY;
    size_t jumbo_threshold = DEFAULT_JUMBO_THRESHOLD;
    bool progress_mode = false;
    unsigned int progress_interval = 5;
    bool force_overwrite = false;
    
    setlocale(LC_NUMERIC, "");

    
    static struct option long_options[] = {
        {"output", required_argument, 0, 'o'},
        {"expected-length", required_argument, 0, 'e'},
        {"jumbo-threshold", required_argument, 0, 'j'},
        {"progress", no_argument, 0, 'p'},
        {"progress-interval", required_argument, 0, 'f'},
        {"color", no_argument, 0, 'c'},
        {"force", no_argument, 0, 'F'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    opterr = 0; // Disable automatic getopt error messages
    while ((opt = getopt_long(argc, argv, "o:e:j:pf:cFh", long_options, NULL)) != -1) {
        switch (opt) {
            case 'o': output_file = optarg; break;
            case 'e': normal_cap = (size_t)atol(optarg); break;
            case 'j': jumbo_threshold = (size_t)atol(optarg); break;
            case 'p': progress_mode = true; break;
            case 'f': progress_interval = (unsigned int)atoi(optarg); break;
            case 'c': use_color = true; break;
            case 'F': force_overwrite = true; break;
            case 'h':
                print_help(argv[0]);
                exit(EXIT_SUCCESS);
            case '?':
                if (optopt == '?') {
                    print_help(argv[0]);
                    exit(EXIT_SUCCESS);
                } else if (optopt != 0) {
                    fprintf(stderr, "%s: invalid option -- '%c'\n", argv[0], optopt);
                } else {
                    fprintf(stderr, "%s: unrecognized option '%s'\n", argv[0], argv[optind-1]);
                }
                fprintf(stderr, "Try '%s --help' for more information.\n", argv[0]);
                exit(EXIT_FAILURE);
            default:
                fprintf(stderr, "Try '%s --help' for more information.\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
    
    int num_files = argc - optind;
    if (num_files < 1) {
        fprintf(stderr, "%s %s Requires at least 1 input file.\n", K_PREFIX, FATAL_PREFIX);
        fprintf(stderr, "Try '%s --help' for more information.\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    
    kStreamState *streams = malloc(sizeof(kStreamState) * num_files);
    if (!streams) { perror("malloc streams"); exit(EXIT_FAILURE); }
    int active_streams = 0;
    unsigned long long total_input_bytes = 0;
    
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
        if (!streams[active_streams].normal_buf) { perror("malloc normal_buf"); exit(EXIT_FAILURE); }
        streams[active_streams].jumbo_buf = NULL;
        streams[active_streams].current_line = streams[active_streams].normal_buf;
        streams[active_streams].capacity = normal_cap;
        streams[active_streams].original_normal_cap = normal_cap;
        streams[active_streams].stream_id = active_streams;
        streams[active_streams].is_jumbo = false;
        streams[active_streams].eof = false;
        streams[active_streams].filename = fname;
        
        struct stat in_st;
        if (fstat(fileno(f), &in_st) == 0) {
            streams[active_streams].st_dev = in_st.st_dev;
            streams[active_streams].st_ino = in_st.st_ino;
            total_input_bytes += in_st.st_size;
        } else {
            streams[active_streams].st_dev = 0;
            streams[active_streams].st_ino = 0;
        }
        
        // Prime explicit tracking sequence cleanly over bounds.
        if (read_next_line(&streams[active_streams], jumbo_threshold)) {
            active_streams++;
        } else {
            fclose(f);
            free(streams[active_streams].normal_buf);
        }
    }
    
    if (active_streams == 0) {
        fprintf(stderr, "%s %s All input files are empty.\n", K_PREFIX, INFO_PREFIX);
        return EXIT_SUCCESS;
    }
    
    FILE *out = stdout;
    if (output_file) {
        struct stat out_st;
        if (stat(output_file, &out_st) == 0) {
            bool is_input = false;
            for (int i = 0; i < active_streams; i++) {
                if (streams[i].st_dev == out_st.st_dev && streams[i].st_ino == out_st.st_ino) {
                    is_input = true;
                    break;
                }
            }
            if (is_input) {
                fprintf(stderr, "%s %s Cowardly refusing to overwrite active input file '%s'\n", K_PREFIX, FATAL_PREFIX, output_file);
                exit(EXIT_FAILURE);
            }
            if (!force_overwrite) {
                fprintf(stderr, "%s %s Output file '%s' already exists. Use --force to overwrite.\n", K_PREFIX, FATAL_PREFIX, output_file);
                exit(EXIT_FAILURE);
            }
        }
    
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
    if (!heap.nodes) { perror("malloc heap.nodes"); exit(EXIT_FAILURE); }
    for (int i = 0; i < active_streams; i++) {
        heap.nodes[i + 1] = i; 
    }
    
    // Explicit topological Heap Initialization O(K) footprint cleanly.
    for (int i = heap.size / 2; i >= 1; i--) {
        heapify_down(&heap, i);
    }
    
    unsigned long long total_emitted = 0;
    unsigned long long total_bytes_emitted = 0;
    time_t start_time = time(NULL);
    time_t last_progress_time = start_time;
    
    while (heap.size > 0) {
        int winner = heap.nodes[1];
        kStreamState *w_stream = &streams[winner];
        
        if (fwrite(w_stream->current_line, 1, w_stream->line_len, out) != w_stream->line_len) {
            perror("fwrite output");
            exit(EXIT_FAILURE);
        }
        total_emitted++;
        total_bytes_emitted += w_stream->line_len;
        
        if (progress_mode) {
            if ((total_emitted & 0xFFFF) == 0) {
                time_t current = time(NULL);
                if (current - last_progress_time >= progress_interval) {
                    time_t total_elapsed = current - start_time;
                    double rows_per_sec = total_elapsed > 0 ? (double)total_emitted / total_elapsed : 0.0;
                    double bytes_per_sec = total_elapsed > 0 ? (double)total_bytes_emitted / total_elapsed : 0.0;
                    
                    char rate_buf[32];
                    format_bytes(bytes_per_sec, rate_buf);
                    
                    double fraction = (total_input_bytes > 0) ? (double)total_bytes_emitted / total_input_bytes : 0.0;
                    time_t remaining = 0;
                    if (fraction > 0.0 && fraction < 1.0) {
                        remaining = (time_t)((total_elapsed / fraction) - total_elapsed);
                    }
                    
                    char eta_buf[64] = "ETA N/A";
                    if (remaining > 0) {
                        int days = remaining / 86400;
                        int hours = (remaining % 86400) / 3600;
                        int mins = (remaining % 3600) / 60;
                        int secs = remaining % 60;
                        if (days > 0) {
                            sprintf(eta_buf, "ETA %d days %02d:%02d:%02d", days, hours, mins, secs);
                        } else {
                            sprintf(eta_buf, "ETA %02d:%02d:%02d", hours, mins, secs);
                        }
                    }
                    char em_buf[32], rate_comma_buf[32];
                    format_commas(total_emitted, em_buf);
                    format_commas((unsigned long long)rows_per_sec, rate_comma_buf);
                    
                    fprintf(stderr, "%s Merged %s rows (%s rows/sec, %s/s) | %s\n", K_PREFIX, em_buf, rate_comma_buf, rate_buf, eta_buf);
                    last_progress_time = current;
                }
            }
        }
        
        if (read_next_line(w_stream, jumbo_threshold)) {
            /* Fast path: if the winner's new line still beats both children,
             * no heap restructuring is needed at all. This is O(1) vs O(log K)
             * and fires on the common case where one file dominates a long run. */
            if (!root_still_wins(&heap)) {
                heapify_down(&heap, 1);
            }
        } else {
            /* Stream exhausted: replace root with the last leaf and sift down. */
            heap.nodes[1] = heap.nodes[heap.size];
            heap.size--;
            if (heap.size > 0) {
                heapify_down(&heap, 1);
            }
        }
    }
    
    if (out != stdout) {
        if (fclose(out) != 0) { perror("fclose output"); exit(EXIT_FAILURE); }
    } else {
        if (fflush(stdout) != 0) { perror("fflush stdout"); exit(EXIT_FAILURE); }
    }
    
    for (int i = 0; i < active_streams; i++) {
        fclose(streams[i].fp);
        free(streams[i].normal_buf);
        if (streams[i].jumbo_buf) free(streams[i].jumbo_buf);
    }
    free(streams);
    free(heap.nodes);
    
    if (progress_mode) {
        time_t end_time = time(NULL);
        time_t total_elapsed = end_time - start_time;
        double overall_rate = total_elapsed > 0 ? (double)total_emitted / total_elapsed : 0.0;
        double overall_bytes_rate = total_elapsed > 0 ? (double)total_bytes_emitted / total_elapsed : 0.0;
        char rate_buf[32];
        format_bytes(overall_bytes_rate, rate_buf);
        char em_buf[32], rate_comma_buf[32];
        format_commas(total_emitted, em_buf);
        format_commas((unsigned long long)overall_rate, rate_comma_buf);
        fprintf(stderr, "%s Merge complete: %s rows merged (%s rows/sec, %s/s) in %lld seconds.\n", K_PREFIX, em_buf, rate_comma_buf, rate_buf, (long long)total_elapsed);
    }
    
    return EXIT_SUCCESS;
}
