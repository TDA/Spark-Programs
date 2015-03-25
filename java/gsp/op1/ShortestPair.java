package gsp.op1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ShortestPair {
	static int count=0;
	static double short_dist=1000000D;
	
	public static void main(String[] args) {
		SparkConf conf= new SparkConf().setAppName("shortpair").setMaster("local[2]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> lines=sc.textFile("C:/testdata/FarthestPairandClosestPairTestData.csv",1);
		JavaPairRDD<Double,Double> points=lines.mapToPair(new PairFunction<String, Double,Double>(){
			public Tuple2<Double,Double> call(String s){
				if(!s.equals("")){
					String[] s1=s.split(",");
					if(s1[0].contains("x"))
						return new Tuple2<Double,Double>(Double.parseDouble("-20000000"),Double.parseDouble("-20000000"));
					
				return new Tuple2<Double,Double>(Double.parseDouble(s1[0]),Double.parseDouble(s1[1]));
				}
				else
					return new Tuple2<Double,Double>(Double.parseDouble("-20000000"),Double.parseDouble("-20000000"));
					
			}
		}).filter(new Function<Tuple2<Double,Double>,Boolean>(){
			public Boolean call(Tuple2<Double,Double> t){
				if(!t.equals(new Tuple2<Double,Double>(-20000000D,-20000000D)))
				return true;
				return false;
			}
			
		});
		points.cache();
		
		Comparator<Tuple2<Double,Double>> comparator=new Comparator<Tuple2<Double,Double>>(){
			 public int compare(Tuple2<Double, Double> tuple1, Tuple2<Double, Double> tuple2) {
			        return tuple1._1 <= tuple2._1 && tuple1._2<=tuple2._2? -1 : 1;
			    }	
		};
		
		
		Function2<Double,Double,Double> max=new Function2<Double,Double,Double>(){
			 public Double call(Double t, Double s) {
			        return t >= s ? t: s;
			    }
		};
		
		Function2<Double,Double,Double> min=new Function2<Double,Double,Double>(){
			 public Double call(Double t, Double s) {
			        return t >= s ? s: t;
			    }
		}; 
		
		Comparator<Double> max1 =new Comparator<Double>() {
			 public int compare(Double tuple1, Double tuple2) {
			        return tuple1 <= tuple2? -1 : 1;
			    }	
		};
		
		
		List<Tuple2<Double,String>> distances_list=new ArrayList<Tuple2<Double,String>>();
		List<Double> distances=new ArrayList<Double>();
		List<Double> shorter_distances=new ArrayList<Double>();
		//HashSet<String> pairs_names=new HashSet<String>();
		Map<String, Double> map = new HashMap<String, Double>();
		
		JavaRDD<Double> maxi=points.keys();
		final double MAXIMUM_VALUE=maxi.reduce(max);
			
		JavaPairRDD<Double,Double>pointsNegative=points.filter(new Function<Tuple2<Double,Double>,Boolean>(){
			public Boolean call(Tuple2<Double,Double> t){
				if(t._1<MAXIMUM_VALUE/2)
				return true;
				else
				return false;
			}
		}).sortByKey();
		
		JavaPairRDD<Double,Double>pointsPositive=points.filter(new Function<Tuple2<Double,Double>,Boolean>(){
			public Boolean call(Tuple2<Double,Double> t){
				if(t._1>=MAXIMUM_VALUE/2)
				return true;
				else
				return false;
			}
		}).sortByKey();
		/*
		for(Tuple2<Double,Double> t: pointsNegative.collect())
			System.out.println(t);
		
		System.out.println("----------------------");
		
		for(Tuple2<Double,Double> t: pointsPositive.collect())
			System.out.println(t);
		*/
		for(Tuple2<Double,Double> t: pointsNegative.take((int)pointsNegative.count())){
			for(Tuple2<Double,Double> s: pointsNegative.take((int)pointsNegative.count())){
				double distance;
				String pair_name=s.toString()+"to"+t.toString();
				if(!t.equals(s)&&!(map.containsKey(pair_name)||map.containsKey(t.toString()+"to"+s.toString()))){
				distance=Farthestpair.compute_distance(t,s);
				map.put(pair_name, distance);
				count++;
				if(!distances.contains(distance)&&distance<short_dist){
				distances_list.add(new Tuple2<Double,String>(distance,pair_name));
				distances.add(distance);
				short_dist=distance;
				}
				}
			}
		}
		
		for(Tuple2<Double,Double> t: pointsPositive.take((int)pointsPositive.count())){
			for(Tuple2<Double,Double> s: pointsPositive.take((int)pointsPositive.count())){
				double distance;
				String pair_name=s.toString()+"to"+t.toString();
				if(!t.equals(s)&&!(map.containsKey(pair_name)||map.containsKey(t.toString()+"to"+s.toString()))){
				distance=Farthestpair.compute_distance(t,s);
				map.put(pair_name, distance);
				count++;
				if(!distances.contains(distance)&&distance<short_dist){
				distances_list.add(new Tuple2<Double,String>(distance,s.toString()+"to"+t.toString()));
				distances.add(distance);
				short_dist=distance;
				}
				}
			}
		}
		
		JavaRDD<Double> distance_rdd=sc.parallelize(distances);
		final double DELTA=Math.sqrt(distance_rdd.reduce(min));
		
		JavaPairRDD<Double,Double>pointsEdgeCases=points.filter(new Function<Tuple2<Double,Double>,Boolean>(){
			public Boolean call(Tuple2<Double,Double> t){
				if(t._1<=MAXIMUM_VALUE/2+DELTA && t._1>=MAXIMUM_VALUE/2-DELTA)
				return true;
				else
				return false;
			}
		}).sortByKey();
		
		
		if(pointsEdgeCases.count()>1){
			for(Tuple2<Double,Double> t: pointsEdgeCases.take((int)pointsEdgeCases.count())){
				for(Tuple2<Double,Double> s: pointsEdgeCases.take((int)pointsEdgeCases.count())){
					double distance;
					if(!t.equals(s)){
					distance=Farthestpair.compute_distance(t,s);	
					count++;
					if(distance<DELTA && !distances.contains(distance)){
					distances_list.add(new Tuple2<Double,String>(distance,s.toString()+"to"+t.toString()));
					shorter_distances.add(distance);
					}
					}
				}
			}
		}
		
		JavaRDD<Double> shorter_distance_rdd=sc.parallelize(shorter_distances);
		double SHORTEST_DIST=DELTA; 
		if(shorter_distance_rdd.count()>0)
		SHORTEST_DIST=Math.min(DELTA, Math.sqrt(shorter_distance_rdd.reduce(min))); 
		System.out.print(SHORTEST_DIST);
		SHORTEST_DIST=Math.pow(SHORTEST_DIST, 2);
		for(Tuple2<Double,String> t:distances_list)
			if(t._1.equals(SHORTEST_DIST))
			System.out.println(t._2);
		System.out.println("Total no of computations "+ distances_list.size()+ " "+ count);			
	}
}