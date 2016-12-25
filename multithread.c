#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <math.h>
#include <signal.h>

// function prototypes
void sighandler();
void * producer_thread_func();
void * consumer_thread_func();
void * performance_stats_thread_func();
int get_msg_size();
useconds_t get_producer_delay();
useconds_t get_consumer_delay();
double normal_distribution();

int TOTAL_RUN_TIME;
int B_BUFFER_SIZE;
int P_COUNT;
int C_COUNT;
float P_t_PRODUCER_DELAY_DIST_PARAM;
float R_s_REQUEST_SIZE_DIST_PARAM;
float C_t1_CONSUMED_WITH_IO_ONLY_DIST_PARAM;
float C_t2_CONSUMED_WITH_IO_DISC_DB_DIST_PARAM;
float P_i_PROBABILITY_OF_C_t1;
int TIME_INTERVAL_FOR_STATUS; //in seconds

pthread_mutex_t lock;
sig_atomic_t quit = 0;

int buffer_index_write = 0;
int buffer_index_read = 0;
int buffer_msg_count = 0;
int* buffer;
int* buffer_msg_sizes;

unsigned long produced_request = 0;
unsigned long processed_request = 0;
unsigned long producer_block = 0;
unsigned long consumer_idle = 0;

struct timeval execution_start_time;
struct timeval execution_end_time;
struct timeval total_execution_time;

double total_execution_t=0.0;
double total_block_t=0.0;
double average_block_t=0.0;
double average_percentage_block_t=0.0;
double percentage_req_processed=0.0;
double percentage_req_blocked=0.0;



int main (int argc,char *argv[]) {
    fprintf (stderr, "Program started\n");
    
    if (argc < 10 || argc > 11){
        fprintf (stderr, "INCORRECT EXECUTION COMMAND \n");
        fprintf (stderr, "USAGE: ./<object_name> <T> <B> <P> <C> <P_t param> <R_s param> <C_t1 param> <C_t2 param> <p_i>\n");
        exit (1);
    } else if (argc == 11) {
        TIME_INTERVAL_FOR_STATUS = atoi(argv[10]);
    } else {
        TIME_INTERVAL_FOR_STATUS = atoi(argv[1])*20/100;
        if (TIME_INTERVAL_FOR_STATUS < 2){
            TIME_INTERVAL_FOR_STATUS = 2;
        }
    } 
    
    if (signal(SIGALRM,sighandler)==SIG_ERR)
       fprintf (stderr, "err");
    if (signal(SIGINT,sighandler)==SIG_ERR)
        fprintf (stderr, "err");
    
    // capturing the command line arguments
    TOTAL_RUN_TIME                              = atoi(argv[1]);
    B_BUFFER_SIZE                               = atoi(argv[2]);
    P_COUNT                                     = atoi(argv[3]);
    C_COUNT                                     = atoi(argv[4]);
    P_t_PRODUCER_DELAY_DIST_PARAM               = atoi(argv[5]);
    R_s_REQUEST_SIZE_DIST_PARAM                 = atoi(argv[6]);
    C_t1_CONSUMED_WITH_IO_ONLY_DIST_PARAM       = atoi(argv[7]);
    C_t2_CONSUMED_WITH_IO_DISC_DB_DIST_PARAM    = atoi(argv[8]);
    P_i_PROBABILITY_OF_C_t1                     = atoi(argv[9])/100.0;
    
    alarm(TOTAL_RUN_TIME);
    
    pthread_t *thread = (pthread_t *)malloc(sizeof(pthread_t)*(P_COUNT+C_COUNT));
    buffer            =       (int *)malloc(sizeof(int)*B_BUFFER_SIZE);
    buffer_msg_sizes  =       (int *)malloc(sizeof(int)*B_BUFFER_SIZE);
    pthread_t performance_stats_thread;
    pthread_mutex_init(&lock, NULL);
    
    // buffer initialization
    for (int i=0; i<B_BUFFER_SIZE; i++){
        buffer[i] = 0;
        buffer_msg_sizes[i] = 0;
    }
    
    fprintf (stderr, "Trace: main: var initialized \n");
    
    pthread_create(&performance_stats_thread,NULL,&performance_stats_thread_func,NULL);
    
    // create Producer Threads
    for (int i=0; i<P_COUNT; i++) {
        pthread_create(&thread[i],NULL,&producer_thread_func,NULL);
    }
    
    // create Consumer Threads
    for (int i=0; i<C_COUNT; i++) {
        pthread_create(&thread[P_COUNT+i],NULL,&consumer_thread_func,NULL);
    }
    
    fprintf (stderr, "Trace: main: all threads created \n");
    gettimeofday(&execution_start_time,NULL);
    
    fprintf (stderr, "Program working.. Please Wait.. \n");
    
    
    
    // to force main to wait till all the threads are done
    for (int i=0; i<P_COUNT; i++){
        int x = pthread_join(thread[i],NULL);
        // fprintf (stderr, "Producers %d ended. exit status: %d \n", i, x);
    }
    // fprintf (stderr, "All Producers Ended \n");
    
    for (int i=0; i<C_COUNT; i++){
        int x = pthread_join(thread[P_COUNT+i],NULL);
        // fprintf (stderr, "Producers %d ended. exit status: %d \n", i, x);
    }
    // fprintf (stderr, "All Consumers Ended \n");
    
    pthread_join(performance_stats_thread,NULL);
    // fprintf (stderr, "performance_stats_thread Ended \n");
    
    // fprintf (stderr, "Trace: main: all threads ended successfully \n");
    
    gettimeofday(&execution_end_time,NULL);
    timersub(&execution_end_time,
             &execution_start_time, 
             &total_execution_time);
        
    // memory cleanup 
    pthread_mutex_destroy(&lock);
    free(thread);
    free(buffer);
    free(buffer_msg_sizes);
    
    // accounting
    total_execution_t           = (double)total_execution_time.tv_sec + (double)total_execution_time.tv_usec/1000000.0;
    average_block_t             = total_block_t/(double)P_COUNT;
    average_percentage_block_t  = average_block_t/total_execution_t*100.0;
    percentage_req_processed    = (double)processed_request/(double)produced_request*100;
    percentage_req_blocked      = (double)producer_block/(double)produced_request*100;
    
    // fprintf (stderr, "\n\n");
    // fprintf (stderr, "total_execution_time              :\t %lf seconds\n", total_execution_t);
    // fprintf (stderr, "average_block_time                :\t %lf seconds\n", average_block_t);
    // fprintf (stderr, "average_percentage_blocked_time   :\t %lf \n", average_percentage_block_t);
    // fprintf (stderr, "produced_request                  :\t %lu \n",produced_request);
    // fprintf (stderr, "processed_request                 :\t %lu \n",processed_request);
    // fprintf (stderr, "producer_block                    :\t %lu \n",producer_block);
    // fprintf (stderr, "consumer_idle                     :\t %lu \n",consumer_idle);
    
    fprintf (stderr, "\n\n");
    fprintf (stderr, "data_collecttion:\n");
    fprintf (stderr, "T\t %d\t", TOTAL_RUN_TIME);
    fprintf (stderr, "B\t %d\t", B_BUFFER_SIZE);
    fprintf (stderr, "P\t %d\t", P_COUNT);
    fprintf (stderr, "C\t %d\t", C_COUNT);
    fprintf (stderr, "Pt\t %0.2f\t", P_t_PRODUCER_DELAY_DIST_PARAM);
    fprintf (stderr, "Rs\t %0.2f\t", R_s_REQUEST_SIZE_DIST_PARAM);
    fprintf (stderr, "Ct1\t %0.2f\t", C_t1_CONSUMED_WITH_IO_ONLY_DIST_PARAM);
    fprintf (stderr, "Ct2\t %0.2f\t", C_t2_CONSUMED_WITH_IO_DISC_DB_DIST_PARAM);
    fprintf (stderr, "Pi\t %0.2f\t", P_i_PROBABILITY_OF_C_t1);
    
    fprintf (stderr, "total_execution_time\t %lf\t", total_execution_t);
    fprintf (stderr, "Time Producers Blocked %%\t %lf\t", average_percentage_block_t);
    fprintf (stderr, "produced_request\t %lu\t", produced_request);
    fprintf (stderr, "processed_request\t %lu\t", processed_request);
    fprintf (stderr, "producer_block\t %lu\t", producer_block);
    fprintf (stderr, "Requests Satisfied %%\t %lf\t", percentage_req_processed);
    fprintf (stderr, "Requests Blocked %%\t %f\n", percentage_req_blocked);
    
    fprintf (stderr, "Program ended\n");
    return 0;
}




// Producer thread function
void * producer_thread_func(){
    struct timeval produced_i_blocked_start_time;
    struct timeval produced_i_blocked_end_time;
    struct timeval produced_i_blocked_total_time;
    struct timeval produced_i_blocked_temp_time;
    produced_i_blocked_total_time.tv_sec=0;
    produced_i_blocked_total_time.tv_usec=0;
    int blocked=0;
    int is_blocked_time=0;
    
    // fprintf (stderr, "Trace: Producer: %d start\n", (int)pthread_self());
    while(quit==0){
        useconds_t t = get_producer_delay(P_t_PRODUCER_DELAY_DIST_PARAM);
        // fprintf (stderr, "Trace: Producer_thread_func: %d delay = %u\n", (int)pthread_self(), t);
        usleep(t);
        
        int msg_size = get_msg_size(R_s_REQUEST_SIZE_DIST_PARAM);
        
        pthread_mutex_lock(&lock);
        produced_request++;
        
        // for sending randomly n number of integers at a time
        if (buffer_msg_count + msg_size  < B_BUFFER_SIZE){
            // for caculateing time 
            if (blocked == 1){
                is_blocked_time = 1;
                gettimeofday(&produced_i_blocked_end_time,NULL);
                blocked = 0;
            }
                
            // fprintf (stderr, "Trace: PRODUCER_data: %d msg_size = %d\n", (int)pthread_self(), msg_size);
            for (int i = 0; i < msg_size; i=((i+1)%B_BUFFER_SIZE)){
                int wr_i = (buffer_index_write+i) % B_BUFFER_SIZE;
                buffer[wr_i] = msg_size; // sending the value of msg_size as msg msg_size number of times
                // fprintf (stderr, "Trace: PRODUCER_data: %d SENT: %d\n",(int)pthread_self(),buffer[wr_i]);
            }
            
            buffer_msg_sizes[buffer_index_write] = msg_size;
            buffer_index_write = (buffer_index_write + msg_size) % B_BUFFER_SIZE;
            buffer_msg_count += msg_size;
            // fprintf (stderr, "Trace: PRODUCER_data: %d buffer_index_write: %d\n",(int)pthread_self(), buffer_index_write);
        }
        else {
            gettimeofday(&produced_i_blocked_start_time,NULL);
            blocked = 1;
            producer_block++;
            // fprintf (stderr, "Trace: PRODUCER_data: %d thread - buffer has NOT ENOUGH SPACE\n",(int)pthread_self());  
        }
        
        if (is_blocked_time == 1){            
            is_blocked_time = 0;
            timersub(&produced_i_blocked_end_time,
                     &produced_i_blocked_start_time, 
                     &produced_i_blocked_temp_time);
            
            timeradd(&produced_i_blocked_total_time,
                     &produced_i_blocked_temp_time, 
                     &produced_i_blocked_total_time);

            total_block_t += (double)produced_i_blocked_temp_time.tv_sec + 
                                (double)produced_i_blocked_temp_time.tv_usec/1000000.0;
        }
            
        pthread_mutex_unlock(&lock);
    }
    
    // fprintf (stderr, "Producer: %d total blocked time =\t %lf \t seconds\n", (int)pthread_self(), 
                                // (double)produced_i_blocked_total_time.tv_sec + 
                                // (double)produced_i_blocked_total_time.tv_usec/1000000.0);

    // fprintf (stderr, "Trace: Producer: %d end\n", (int)pthread_self());
    pthread_exit(NULL);
}




// Consumer thread function
void * consumer_thread_func (){
   // fprintf (stderr, "Trace: Consumer: %d start\n", (int)pthread_self());
    while (quit==0) {
        pthread_mutex_lock(&lock);
        // fprintf (stderr, "Trace: consume_data: %d start\n", (int)pthread_self());
        
        // for sending randomly n number of integers at a time
        if (buffer_msg_count > 0){
            int rev_data;
            int msg_size = buffer_msg_sizes[buffer_index_read];
            // fprintf (stderr, "Trace: consume_data: %d RECEIVED msg_size = %d\n", (int)pthread_self(), msg_size);
            
            for (int i=0; i < msg_size; i=((i+1)%B_BUFFER_SIZE)){
                int rd_i = (buffer_index_read + i) % B_BUFFER_SIZE;
                rev_data = buffer[rd_i];
                // fprintf (stderr, "Trace: consume_data: %d RECEIVED: %d\n",(int)pthread_self(),rev_data);
            }
            
            // simulating time to consume data
            useconds_t t =get_consumer_delay(P_i_PROBABILITY_OF_C_t1);
            usleep(t);
            
            buffer_msg_sizes[buffer_index_read] = 0;
            buffer_index_read = (buffer_index_read + msg_size) % B_BUFFER_SIZE;
            buffer_msg_count -= msg_size;
            
            // fprintf (stderr, "Trace: consume_data: %d buffer_index_read: %d\n",(int)pthread_self(),buffer_index_read);
            
            processed_request++;
        } else {
            consumer_idle++;
            // fprintf (stderr, "Trace: consume_data: %d thread - buffer has NO DATA\n",(int)pthread_self()); 
        }    
        pthread_mutex_unlock(&lock);
        // fprintf (stderr, "Trace: consume_data: %d end\n", (int)pthread_self());  
        
    }
    // fprintf (stderr, "Trace: Consumer: %d end\n", (int)pthread_self());
    pthread_exit(NULL);
}




void * performance_stats_thread_func (){
    struct timeval current_time;
    struct timeval elapsed_time;
    int printed=0;
    int current_sec=0;
    int old_printed_sec=0;
    while (quit==0) {
        sleep(TIME_INTERVAL_FOR_STATUS);
        gettimeofday(&current_time,NULL);
        timersub(&current_time, &execution_start_time, &elapsed_time);
                
        pthread_mutex_lock(&lock);
        
        // fprintf (stderr, "Time: %lf seconds\tproduced: %lu\tprocessed: %lu\tblocked: %lu\n", 
                                    // ((double)elapsed_time.tv_sec + (double)elapsed_time.tv_usec/1000000.0),
                                    // produced_request,
                                    // processed_request,
                                    // producer_block);
        
        pthread_mutex_unlock(&lock);
    }
    pthread_exit(NULL);
}





useconds_t get_producer_delay(double P_t_param){
    return (useconds_t)(100000.0*normal_distribution (P_t_param));
}


useconds_t get_consumer_delay(double probability){
    double x;
    srand(time(NULL)*(int)pthread_self());
    x=(double)rand()/(double)(RAND_MAX);
    if(x<=probability) {
        return (useconds_t)(100000.0*normal_distribution(C_t1_CONSUMED_WITH_IO_ONLY_DIST_PARAM));
    } else {
        return (useconds_t)(1000000.0*normal_distribution(C_t2_CONSUMED_WITH_IO_DISC_DB_DIST_PARAM));
    }
}



int get_msg_size(int req_size){
    int c;
    int r;
    srand(time(NULL)*(int)pthread_self()+(int)req_size);
    c=rand();
    if(c!=0){
         r= (int)((c % B_BUFFER_SIZE) % 20);
         if (r == 0){
            get_msg_size(req_size);
         } else {               
            return abs(r);
         }
    } else {
        get_msg_size(req_size);
    }
}


// assuming mu = 0 for this normal distribution function
// and sigma is the param that have been entered in the cmd line
double normal_distribution (double sigma) {
    double x,y;
    srand(time(NULL)*(int)pthread_self());
    x = (double)rand()/(double)(RAND_MAX)*sigma;
    if(x!=0) {
        y=(exp(-(x*x)/(2*sigma*sigma)))/(sqrt(2*3.1416)*sigma);
        return y;
    } else {
        normal_distribution(sigma);
    }
}


void sighandler(int signum) {
    if (signum==SIGINT){
        fprintf (stderr, "Trace: SIGNAL = SIGINT \n");
        quit=1;
    }
    if (signum==SIGALRM){
        fprintf (stderr, "Trace: SIGNAL = SIGALRM after %d seconds\n", TOTAL_RUN_TIME);
        quit=1;
    }
}

