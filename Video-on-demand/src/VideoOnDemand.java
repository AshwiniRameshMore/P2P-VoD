

import java.util.*;

public class VideoOnDemand
{
	static int no_of_requests =2750;
	static double skew = 0.7; //70% of all accesses were directed to 30% percent of files.
	static int no_of_movies = 500;
	static int no_of_serving_peers = 100;
	static double peer_array[][] = new double[no_of_serving_peers][5];
	static int time_span = 50;
	static double base_size = 1000.0;
	static double movie_array[][] = new double[no_of_movies][10];
	static double map_array[][] = new double[no_of_movies*no_of_serving_peers][10];
	static double distance_array[][] = new double[no_of_requests][10];
	static double High_transfer_rate=31.0; // Kbytes/sec;

   public static void main(String[] args)
   {
// Calculate each file's popularity weight and store it into the Movie array.
			double zeta;
			zeta=Math.log(skew)/Math.log(1-skew);
			double c = 0.0;
			int i = 0;
	        int a = no_of_movies;
		    while(i<a)
			{
					c = c+Math.pow((i+1),zeta-1);
					movie_array[i][0] = 1/(Math.pow((double)(i+1),(1-zeta)));
					movie_array[i][4] = i + 1;
					i++;
			}
			c=1/c;
			for(i=0;i<a;i++)
			{
					//Popularity of each file
					movie_array[i][0]=truncate(c*movie_array[i][0],5);
			}

//*************************************************************
//Calculate access rate
		    i=0;
			while(i<no_of_movies)
			{
				movie_array[i][1]=truncate(no_of_requests*movie_array[i][0]/time_span,5);
				movie_array[i][2]=truncate(Math.floor(no_of_requests*movie_array[i][0]),5);//totals=no_of_requests
				movie_array[i][3]=Math.round(base_size*movie_array[i][2]);
				i++;
			}
			for(i=0;i<no_of_movies;i++)
			{
				for(int j=1;j<no_of_movies;j++)
				{
					if(movie_array[i][3]==movie_array[j][3])
			  		 movie_array[i][3]=movie_array[i][3]+new Random().nextInt(no_of_movies);
				}
			}
//*************************************************************
//Generate storage space for peers
		for(int j=0;j<no_of_serving_peers;j++)
		{
			peer_array[j][0]=j+1;
			double index = Math.random() * 50;
			if(index<=0)
			  peer_array[j][1]=1*base_size*10000;
			else
			  peer_array[j][1]=Math.round(index*base_size*100);
		}
		
//*************************************************************
//Compute average(popularity,access_rate) for each and store in movie_array
		i=0;
		while(i<no_of_movies)
		{
				movie_array[i][4]=truncate((movie_array[i][0]+movie_array[i][1])/2,5);
				i++;
		}
//*************************************************************
//Calculate no of replicas=average* no_of_serving_peers
		boolean swap=true;
		int j=0;
		double temp;
		while(swap){
			swap=false;
			j++;
			for(int i1=0;i1<peer_array.length-j;i1++)
			{
				if(peer_array[i1][1]>peer_array[i1+1][1])
				{
					temp=peer_array[i1][1];
					peer_array[i1][1]=peer_array[i1+1][1];
					peer_array[i1+1][1]=temp;
					swap=true;
				}
			}
		}
		for(int k=0;k<no_of_serving_peers;k++)
			peer_array[k][2]=peer_array[k][1];
		i=0;
		while(i<no_of_movies)
		{
		     double size=movie_array[i][3];
			movie_array[i][5]=0.0;
			for(int k=0;k<peer_array.length;k++){
				if(peer_array[k][2] >= (peer_array[k][2]-size) && (peer_array[k][2]-size)>0)
				  movie_array[i][5]+=1.0;
			}		
			i++;
		}
		i=0;
		while(i<no_of_movies)
		{
			double no_of_available_peers= movie_array[i][5];
			int replicas=(int)(movie_array[i][4]*no_of_available_peers);//avg*avail_peers
			if(replicas > no_of_available_peers)
			      movie_array[i][5]=no_of_available_peers;
			else if(replicas < 1)
				  movie_array[i][5]=1;
			else
				  movie_array[i][5]=replicas;	
		    i++;
		}

//*************************************************************
//Calculate weight for each movie =(access_rate * popularity)/no_replica
		
for(i=0;i<no_of_movies;i++)
	movie_array[i][6]=truncate((movie_array[i][2]*movie_array[i][0])/movie_array[i][5],5);
		
//*************************************************************
//Select serving peers whose space > movie_size

for(i=0;i<no_of_movies;i++)
{
	int no_replica = (int)movie_array[i][5];
	double size=movie_array[i][3];
	for(int k=0;k<peer_array.length;k++)
	{
		if(peer_array[k][2] >= (peer_array[k][2]-movie_array[i][3]) && (peer_array[k][2]-movie_array[i][3])>0)
		{ 
			putMap(peer_array[k][0],movie_array[i][3],map_array,peer_array[k][3]);
			peer_array[k][2] = peer_array[k][2] - size;	
			no_replica--;
			if(no_replica<1)
			break;
		}
	}
}
double temp1,temp2,temp3,temp4;
boolean swap1=true;
int j1=0;

while(swap1){
	swap1=false;
	j1++;
	for(int i1=0;i1<map_array.length-j1;i1++)
	{
		if(map_array[i1][2]>map_array[i1+1][2] && map_array[i1][1]==map_array[i1+1][1])
		{
		    temp2=map_array[i1][2];
			map_array[i1][2]=map_array[i1+1][2];
			map_array[i1+1][2]=temp2;
			temp3=map_array[i1][1];
			map_array[i1][1]=map_array[i1+1][1];
			map_array[i1+1][1]=temp3;
			temp4=map_array[i1][0];
			map_array[i1][0]=map_array[i1+1][0];
			map_array[i1+1][0]=temp4;
			swap1=true;
		}
	}
}
//**********************************************************
//Generate requests
int sum1=0;
for(i=0;i<movie_array.length;i++)
	sum1 = sum1 + (int)movie_array[i][2];

int Real_request_num = sum1;
i=0;
while(i<no_of_movies){
	movie_array[i][1]=movie_array[i][2]/time_span;
	i++;
}
double incoming_requests[][]= new double[Real_request_num][10];

i=0;
int start_point=0;
int Total_num_request_currentfile=0;
while( i <no_of_movies)
{
  	    double lambda=movie_array[i][1];
	    Total_num_request_currentfile=(int)movie_array[i][2]; 
	    double Request_arrival_time = 1/lambda;
	    j=0;	   
	    while (j < Total_num_request_currentfile)
	    {
	    	Request_arrival_time=Request_arrival_time+1/lambda;
	        incoming_requests[start_point][0]=movie_array[i][3];
	        incoming_requests[start_point][2]=Request_arrival_time;
	        start_point=start_point+1;   
	        j=j+1;
	    }
	    i=i+1;
}
swap=true;
j=0;
while(swap){
	swap=false;
	j++;
	for(int i1=0;i1<incoming_requests.length-j;i1++)
	{
		if(incoming_requests[i1][2]>incoming_requests[i1+1][2])
		{
			temp=incoming_requests[i1][0];
			incoming_requests[i1][0]=incoming_requests[i1+1][0];
			incoming_requests[i1+1][0]=temp;
			temp1=incoming_requests[i1][1];
			incoming_requests[i1][1]=incoming_requests[i1+1][1];
			incoming_requests[i1+1][1]=temp1;
			temp2=incoming_requests[i1][2];
			incoming_requests[i1][2]=incoming_requests[i1+1][2];
			incoming_requests[i1+1][2]=temp2;
			swap=true;
		}
	}
}

//**********************************************************
//Least load First

Least_load_first(incoming_requests,Real_request_num);

//**********************************************************
//CoRe

Core(incoming_requests, Real_request_num);

//**********************************************************
//Calculating aggregate access rate & response times for both algos
double sum_llf=0, sum_core=0;
for(i=0;i<incoming_requests.length;i++)
{
	sum_llf+=incoming_requests[i][3];
	sum_core+=incoming_requests[i][4];
}
double sum=0;
for(i=0;i<movie_array.length;i++){
	sum+=movie_array[i][1];
}
System.out.println("\nAggregate access rate:"+sum);
System.out.println("\nResponse time for Least Load First:    "+sum_llf/no_of_requests+" seconds");
System.out.println("Response time for CoRe:   "+sum_core/no_of_requests+" seconds");
if(sum_llf/no_of_requests > sum_core/no_of_requests)
	System.out.println("Success!!");
else
	System.out.println("Fail!!");
/*System.out.println("**********************************************************");
System.out.println("MOVIE_ARRAY");
display_movie_array();

System.out.println("**********************************************************");
System.out.println("PEER_ARRAY");
display_peer_array();

System.out.println("**********************************************************");
System.out.println("MAP_ARRAY");
display_map_array();

System.out.println("**********************************************************");
System.out.println("INCOMING_REQS");
display_incoming_requests(incoming_requests);*/
}//main

private static void Least_load_first(double[][] incoming_requests, int real_request_num) 
{
    int i = 0;
	double req_size = 0.0;
	double current_req_arrival_time=0.0;
	double current_req_service_time=0.0;
	double request_response_time=0.0;
	double request_finish_time=0.0;
	double idle_time=0.0;
	double peer_available_time;
	double request_start_time = 0;
	while(i < real_request_num)
	{
	 	req_size = incoming_requests[i][0];
	 	current_req_arrival_time=incoming_requests[i][2];
	 	for(int j = 0; j < map_array.length; j++)
	 	{
	 	 if(req_size == map_array[j][1])//if incoming request of movie == movie in map_array(placed on peer)
	 	 {
	 		 for(int o=0;o<peer_array.length;o++)
	 		 {
	 		   peer_available_time=peer_array[o][3];
			   idle_time = current_req_arrival_time - peer_available_time;
				if (idle_time <= 0)
					request_start_time = current_req_arrival_time; 
				else
					request_start_time = peer_available_time; 
				
				current_req_service_time=req_size/High_transfer_rate;
				request_finish_time=request_start_time + current_req_service_time;
			    peer_array[o][3] = request_finish_time;  
	 		    break;
	 	     }
	 	  }
	 	}
	 	request_response_time=request_finish_time-request_start_time;
	 	incoming_requests[i][3]=request_response_time;
	 	i++;
	 }
}

private static void Core(double[][] incoming_requests, int real_request_num) 
{
	int i=0;
	int j;
	for(int y=0;y<peer_array.length;y++)
		peer_array[y][3]=0.0;
	while(i < real_request_num)
	{
		ArrayList<Double> list=new ArrayList<Double>();
		double distance_array[][] = new double[no_of_requests][10];
		for(j=0;j<map_array.length;j++)
		{
			if(incoming_requests[i][0]==map_array[j][1])
			{
				list.add(map_array[j][0]);
			}
		}
		List<Double> dist= new ArrayList<Double>();
		for(int q=0;q<list.size();q++)
		{
			distance_array[q][0]=list.get(q);//peer_no on which movie req is available
			double index=new Random().nextInt(list.size()+20); 
			if(index>0.0 && index<no_of_serving_peers)
			    dist.add(index);  //distance of peer from source
			else
				dist.add((double) list.size());  	
		}
		Collections.sort(dist);
		for(int q=0;q<dist.size();q++)
			distance_array[q][1]=dist.get(q);
		
		List<Double> result= new ArrayList<Double>();
		double total = 1.0;
		for (int i9 = 0;i9 < list.size()-1;i9++) 
		{
		    double db = distance_array[i9][1]/no_of_serving_peers;
		    result.add(db);
		    total -= db;
		}
		result.add(total);
		Collections.sort(result);
		Collections.reverse(result);
	     for(int k=0;k<list.size();k++)
	    	 distance_array[k][2]=result.get(k);

	    double request_size=incoming_requests[i][0];
		double current_req_arrival_time=0.0;
		double current_req_service_time=0.0;
		double request_response_time=0.0;
		double request_finish_time=0.0;
	    current_req_service_time=0.0;
		current_req_arrival_time=incoming_requests[i][2];
	    double idle_time=0.0;
	    double peer_available_time;
	    double request_start_time = 0;
	    List<Double> result_service_time= new ArrayList<Double>();
	
	  for(int k=0;k<list.size();k++)
	  {
		  for(int k1=0;k1<peer_array.length;k1++) 
		  {
			  if(distance_array[k][0]==peer_array[k1][0])
			  {
				  peer_available_time=peer_array[k1][3];
				  idle_time = current_req_arrival_time - peer_available_time;
					
					if (idle_time <= 0)
						request_start_time = current_req_arrival_time; 
					else
						request_start_time = peer_available_time; 
					
					result_service_time.add((request_size*distance_array[k][2])/High_transfer_rate);
					request_finish_time=request_start_time + current_req_service_time;
				    peer_array[k1][3] = request_finish_time;  
			  }
		  }
	  }
	  if(!result_service_time.isEmpty())
		  current_req_service_time=Collections.max(result_service_time);
		request_finish_time=request_start_time + current_req_service_time;
		request_response_time = request_finish_time-request_start_time;
		incoming_requests[i][4] = request_response_time;
		i++;
	/*	System.out.println("**********************************************************");
		System.out.println("INCOMING_REQS Distance array");
		display_Distance_array(list.size(),request_size,distance_array);   */
	}
}

private static void display_Distance_array(int listSize, double request_size, double[][] distance_array) 
{
	int m=0;
	System.out.println("--------------------------------------------------------------------------------------");
	System.out.println("ReqNo Peer_no Distance Share(%) ");
	System.out.println("--------------------------------------------------------------------------------------");
	while(m<listSize)
	{ 	
		System.out.print(request_size);
	    System.out.print("   ");
		System.out.print(distance_array[m][0]);
	    System.out.print("   ");
	    System.out.print(distance_array[m][1]);
	    System.out.print("   ");
	    System.out.println(distance_array[m][2]);
	m++;
	}
}

private static void display_incoming_requests(double[][] incoming_requests) 
{
		int i=0;
		System.out.println("--------------------------------------------------------------------------------------");
		System.out.println("Req_for Arrival_time          llf             Core");
		System.out.println("--------------------------------------------------------------------------------------");
		while(i<incoming_requests.length)
		{ 	
			System.out.print(incoming_requests[i][0]);
		    System.out.print("   ");
		    System.out.print(incoming_requests[i][2]);
		    System.out.print("                  ");
		    System.out.print(incoming_requests[i][3]);
		    System.out.print("               ");
		    System.out.println(incoming_requests[i][4]);
		    i++;
		}
}

private static void display_map_array() 
{
	int i=0;
	System.out.println("--------------------------------------------------------------------------------------");
	System.out.println("Peer_no  Movie_Size SLF_peer_avl_time");
	System.out.println("--------------------------------------------------------------------------------------");
	while(i<map_array.length)
	{ 	if(map_array[i][0]!=0.0 && map_array[i][1]!=0.0)
	    {
		System.out.print(map_array[i][0]);
	     System.out.print("   ");
	    System.out.print(map_array[i][1]);
	    System.out.print("   ");
	    System.out.println(map_array[i][2]);
	    }
	i++;
	}
}

private static void display_peer_array() 
{
	int i=0;
	System.out.println("--------------------------------------------------------------------------------------");
	System.out.println("Peer_no  Total_Storage  Storage_left Peer_Avl_time");
	System.out.println("--------------------------------------------------------------------------------------");
	while(i<no_of_serving_peers)
	{ 	
		System.out.print(peer_array[i][0]);
	    System.out.print("   ");
	    System.out.print(peer_array[i][1]);
	    System.out.print("   ");
	    System.out.print(peer_array[i][2]);
	    System.out.print("   ");
	    System.out.print(peer_array[i][3]);
	    System.out.print("   ");
	    System.out.println(peer_array[i][4]);
	  i++;
	}
}

private static void display_movie_array() 
{
	int i=0;
	System.out.println("--------------------------------------------------------------------------------------");
	System.out.println("Popularity   AccessRate/timespan    Total      Size           Average        Replicas    MW");
	System.out.println("--------------------------------------------------------------------------------------");
	while(i<no_of_movies)
	{
		System.out.print(movie_array[i][0]);
		System.out.print("       ");
		System.out.print(movie_array[i][1]);
		System.out.print("                ");
		System.out.print(movie_array[i][2]);
		System.out.print("      ");
		System.out.print(movie_array[i][3]);
		System.out.print("          ");
		System.out.print(movie_array[i][4]);
		System.out.print("           ");
		System.out.print(movie_array[i][5]);
		System.out.print("           ");
		System.out.println(movie_array[i][6]);
		i++;
	}
}

static int var = -1;
private static void putMap(double d, double e, double[][] map_array, double peer_array2) {
	var++;
	map_array[var][0]=d;
	map_array[var][1]=e;
	map_array[var][2]=peer_array2;	
}

public static double truncate(double number, int precision)
{
     double prec = Math.pow(10, precision);
     int integerPart = (int) number;
     double fractionalPart = number - integerPart;
     fractionalPart *= prec;
     int fractPart = (int) fractionalPart;
     fractionalPart = (double) (integerPart) + (double) (fractPart)/prec;
     return fractionalPart;
 }
}//class



