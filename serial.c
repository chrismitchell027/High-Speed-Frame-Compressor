#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <pthread.h>


#define BUFFER_SIZE 1048576 // 1MB


// struct for output data array
typedef struct {
	int nbytes_zipped;
	unsigned char buffer_out[BUFFER_SIZE];
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int flag;
	
} zipped_frame;
/*


pthread_mutex_t lock;
assert(pthread_mutex_init ( &lock, NULL) == 0);


*/
/*

pthread mutex lock
pthread cond cond
int flag = 0

void producer() {
	lock the lock

	do the work

	flag = 1
	signal the consumer

	unlock the lock
}


void consumer() {
	lock the lock
	
	while (flag == 0) {
		pthread_cond_wait(&cond, &lock)
	}
	consume and write to output file
	unlock the lock
}
*/

// global variables for the files array
char **files;
int global_index = 0;
int nfiles = 0;
pthread_mutex_t index_lock;

// global variables for the output array
zipped_frame *frames = NULL;

// global variables for total_in and total_out bytes
int total_in = 0;
int total_out = 0;
pthread_mutex_t total_in_lock, total_out_lock;

int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

// broke down main function into separate components
// init_files creates a list of sorted PPM files
void init_files(const char* folder_name) {
	DIR *d;
	struct dirent *dir;
	d = opendir(folder_name);
	assert(d != NULL);

	// create sorted list of PPM files
	while ((dir = readdir(d)) != NULL) {
		files = realloc(files, (nfiles+1)*sizeof(char *));
		assert(files != NULL);

		int len = strlen(dir->d_name);
		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 'p' && dir->d_name[len-2] == 'p' && dir->d_name[len-1] == 'm') {
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	// sort the files 
	qsort(files, nfiles, sizeof(char *), cmp);

	// initialize index_lock
	assert(pthread_mutex_init(&index_lock, NULL) == 0);
}

void clean_files() {
	// check that files exists
	if (files == NULL)
		return;
	// free memory for each file in files
	for(int i=0; i < nfiles; i++)
		free(files[i]);
	// free memory for files
	free(files);
	// avoid dangling pointer
	files = NULL;
}

void init_frames() {
	frames = (zipped_frame *)malloc(nfiles * sizeof(zipped_frame));
	assert(frames != NULL);
	for (int i = 0; i < nfiles; i++) {
		assert(pthread_mutex_init(&frames[i].lock, NULL) == 0);
		assert(pthread_cond_init(&frames[i].cond, NULL) == 0);
		frames[i].flag = 0;
	}
}

void clean_frames() {
	// check that frames exists
	if (frames == NULL)
		return;
	// free memory for each individual frame
	for (int i = 0; i < nfiles; i++) {
		pthread_mutex_destroy(&frames[i].lock);
		pthread_cond_destroy(&frames[i].cond);
	}
	// free memory for frames
	free(frames);
	// avoid dangling pointer
	frames = NULL;
}

void clean_locks() {
	pthread_mutex_destroy(&total_in_lock);
	pthread_mutex_destroy(&total_out_lock);
	pthread_mutex_destroy(&index_lock);
}

// worker thread -> zips and dumps data to our frames array for the output thread to use
void* process_files(void* arg) {
	while(1) {
		// critical section used to determine index of next file to be processed
		pthread_mutex_lock(&index_lock);
		
			// check if all files have been processed
			//	- if so, unlock and break the loop
			if (global_index >= nfiles) {
				pthread_mutex_unlock(&index_lock);
				break;
			}

			// at this point, we know we have another file to process
			
			// we will process the file at the current global_index value
			int index = global_index;
			// increment global_index for the next thread
			global_index++;
			// unlock index_lock
		pthread_mutex_unlock(&index_lock);

		// at this point, we can start processing our selected file

		// calculate the full file path
		int len = strlen((char *) arg) + strlen(files[index]) + 2;
		char* full_path = malloc(len*sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, (char *) arg);
		strcat(full_path, "/");
		strcat(full_path, files[index]);

		// create buffers
		unsigned char buffer_in[BUFFER_SIZE];
		unsigned char buffer_out[BUFFER_SIZE];

		// load file
		FILE *f_in = fopen(full_path, "r");
		assert(f_in != NULL);
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);

		// protect critical section with lock
		pthread_mutex_lock(&total_in_lock);
			total_in += nbytes;
		pthread_mutex_unlock(&total_in_lock);

		// zip file
		z_stream strm;
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);

		// dump zipped file
		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
		
		// SAVE OUTPUT BYTES (protect this section with lock)
		pthread_mutex_lock(&frames[index].lock);
			// copy nbytes and the buffer over to frames[index]
			frames[index].nbytes_zipped = nbytes_zipped;
			memcpy(frames[index].buffer_out, buffer_out, nbytes_zipped);
			// mark frames[index] as completed and signal the consumer thread to wake up
			frames[index].flag = 1;
			pthread_cond_signal(&frames[index].cond);
		pthread_mutex_unlock(&frames[index].lock);

		// protect critical section with lock
		pthread_mutex_lock(&total_out_lock);
			total_out += nbytes_zipped;
		pthread_mutex_unlock(&total_out_lock);

		free(full_path);
	}
	return NULL;
}

// consumer thread
void* consume_frames(void* arg) {
	FILE *f_out = fopen("video.vzip", "w");
	assert(f_out != NULL);
	for (int i = 0; i < nfiles; i++) {
		pthread_mutex_lock(&frames[i].lock);
			while (!frames[i].flag)
				pthread_cond_wait(&frames[i].cond, &frames[i].lock);

			fwrite(&frames[i].nbytes_zipped, sizeof(int), 1, f_out);
			fwrite(frames[i].buffer_out, sizeof(unsigned char), frames[i].nbytes_zipped, f_out);
		pthread_mutex_unlock(&frames[i].lock);
	}
	fclose(f_out);
}

int main(int argc, char **argv) {
	// time computation header
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	// end of time computation header

	// do not modify the main function before this point!
	assert(argc == 2);

	// main thread initializes sorted list of PPM files
	// stored in "files"
	// size of array is stored in "nfiles"
	// also initializes the index_lock used for our global_index integer
	init_files(argv[1]);

	// initialize the frames array
	// - this is used to store processed frames (produced by workers)
	// - this is only consumed by 1 thread which writes to the output file
	init_frames();

	// create a single zipped package with all PPM files in lexicographical order
	
	// initialize total_in and total_out locks
	assert(pthread_mutex_init(&total_in_lock, NULL) == 0);
	assert(pthread_mutex_init(&total_out_lock, NULL) == 0);

	pthread_t p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, c1;
	assert(pthread_create(&p1, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p2, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p3, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p4, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p5, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p6, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p7, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p8, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p9, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p10, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p11, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p12, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p13, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p14, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p15, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p16, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p17, NULL, &process_files, argv[1]) == 0);
	assert(pthread_create(&p18, NULL, &process_files, argv[1]) == 0);

	assert(pthread_create(&c1, NULL, &consume_frames, NULL) == 0);



	// ALL WORKERS MUST BE COMPLETELY FINISHED AT THIS POINT
	assert(pthread_join(p1, NULL) == 0);
	clean_files();

	// CONSUMER MUST BE COMPLETELY FINISHED AT THIS POINT
	assert(pthread_join(c1, NULL) == 0);
	clean_frames();

	// clean other locks
	clean_locks();

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);
	// do not modify the main function after this point!

	// time computation footer
	clock_gettime(CLOCK_MONOTONIC, &end);
	printf("Time: %.2f seconds\n", ((double)end.tv_sec+1.0e-9*end.tv_nsec)-((double)start.tv_sec+1.0e-9*start.tv_nsec));
	// end of time computation footer

	return 0;
}
