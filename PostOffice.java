import static java.lang.Thread.sleep;
import java.util.concurrent.*;
import java.util.*;

class PostOffice extends Thread {
    public static Semaphore customer_capacity=new Semaphore(10); //limit the number of customers to 10 in the room
    public static Semaphore workers=new Semaphore(3); //there are three workers, create three threads
    public static Semaphore mutex1=new Semaphore(1); //protect the reading and writing the the queues
    public static Semaphore scale=new Semaphore(1);  //protect the use of scale by only one worker
    public static Semaphore cust_ready=new Semaphore(0); //to signal customer is ready
    public static Semaphore leave_post_office=new Semaphore(0); //to signal customer left post office
    public static Semaphore[] finished=new Semaphore[50]; //create a separate semaphore for every customer
    static {
        for(int i=0; i<50; i++) finished[i]=new Semaphore(0);
    }
    public static int customerID=0;
    public static int workerID=0;  
    public static Queue<Integer> queue1=new LinkedList<Integer>(); //store the customer ID number
    public static Queue<Integer> queue2=new LinkedList<Integer>(); //store customer task number
    
    public void run(){
        final int workersize=3; //three poster workers
        final int customersize=50; //50 customers
        Thread customerThread[]=new Thread[customersize]; 
        Thread workerThread[]=new Thread[workersize];
        System.out.println("Simulating Post Office with 50 customers and 3 postal workers");
        System.out.println();
        
        //create threads for every poster workers
        for(int i=0; i<workersize; i++){
            workerThread[i] =new Thread(new Worker(workerID));
            workerThread[i].start();
            workerID++;                        
        }
        
        //create threads for every customers
        for(int i=0; i<customersize; i++){
            customerThread[i] =new Thread(new Customer(customerID));
            customerThread[i].start();
            customerID++;
        }
        
        //force all the threads to finish before going back to main thread
        try{
            for(int i=0; i<customersize; i++){
                customerThread[i].join();
                System.out.println("Joined customer "+i);
            }
            for(int i=0; i<workersize; i++){
                workerThread[i].join();
                System.out.println("Joined poster worker "+i);
            }
            }catch (InterruptedException e){e.printStackTrace();}
        }

    public static void main(String[] args){
        PostOffice postoffice=new PostOffice();
        
        //set the postoffice as a thread
        postoffice.start();
        try{
            postoffice.join();
        }catch (InterruptedException e){e.printStackTrace();}
    }


    private class Customer implements Runnable{
        private final int id;

        //Customer constructor, initialize the customer id
        Customer(int id){
        this.id=id;
        System.out.println("Customer "+id+" created");
    }

        @Override
        public void run() {
            try {
                customer_capacity.acquire();
                enter_post_office();
                workers.acquire();
                mutex1.acquire();
                queue1.add(id);
                queue2.add(assignTask());
                mutex1.release();
                cust_ready.release();
                finished[id].acquire();
                leave_post_office.release();
                leavePostOffice();
                customer_capacity.release();                
            } catch (InterruptedException ex) {}
            
        }

        private void enter_post_office(){
            System.out.println("Customer "+id+" enters post office");
        }
        
        private int assignTask(){
            int taskchoice=(int)(Math.random()*3 + 1); 
            return taskchoice;
        }
        
        private void leavePostOffice(){
        	System.out.println("Customer "+id+" left the post office.");
        }
    }

    private class Worker implements Runnable{
        private int id;
        private int p_cust;
        private int tasknumber; 

        Worker(int id){
            this.id=id;
            System.out.println("Postal Worker "+id+" created");
        }
        
        private void task(){
            
            try {                
                switch(tasknumber){
                    case 1:                       
                       sleep(1000);
                       System.out.println("Customer "+p_cust+" asks poster worker "+id+" to buy stamps");
                       break;
                    case 2:
                       sleep(1500);
                       System.out.println("Customer "+p_cust+" asks poster worker "+id+" to mail a letter");
                       break;
                    case 3:
                       sleep(1000);
                       System.out.println("Customer "+p_cust+" asks poster worker "+id+" to mail a package");
                       scale.acquire();
                       System.out.println("Scale in use by poster worker "+id);
                       sleep(1000);
                       System.out.println("Scale released by poster worker "+id);
                       scale.release();
                       break;
                } 
            }catch (InterruptedException ex) {}           
        }
        
        @Override
        public void run() {
            
            try{    
                    boolean flag=true;
	            while(flag==true){
	                cust_ready.acquire();                       
	                mutex1.acquire();
	                p_cust=queue1.remove();
	                tasknumber=queue2.remove();                        
	                mutex1.release();
                        System.out.println("Poster worker "+id+" serving customer "+p_cust);
	                task();
                        System.out.println("Poster worker "+id+" finished serving customer "+p_cust);
	                finished[p_cust].release();
	                leave_post_office.acquire();                 
	                workers.release();
                        if(queue1.isEmpty())flag=false;
	            }
            }catch(InterruptedException ex) {} 
        }
    }

}