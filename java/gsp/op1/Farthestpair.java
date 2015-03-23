package gsp.op1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Farthestpair {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf= new SparkConf().setAppName("c_hull").setMaster("local[2]");
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
		List<Tuple2<Double,Double>> l=points.sortByKey().collect();
		Collections.sort(l,comparator);
		List<Tuple2<Double,Double>> convex_hull=new ArrayList<Tuple2<Double,Double>>();
		List<Tuple2<Double,Double>> leftSet=new ArrayList<Tuple2<Double,Double>>();
		List<Tuple2<Double,Double>> rightSet=new ArrayList<Tuple2<Double,Double>>();
		
		convexhull1.compute_hull(l, convex_hull, rightSet, leftSet);
		Tuple2<Double,Double> pt1,pt2;
		if(convex_hull.size()==2){
			pt1=convex_hull.get(0);
			pt2=convex_hull.get(1);
			double distance=compute_distance(pt1,pt2);
			String p2p=pt1+" to "+pt2;
			System.out.println("The farthest points are "+p2p+" with distance "+ distance);
			System.exit(0);
		}
		
		JavaPairRDD<String,Double> distances=null;
		List<Tuple2<String,Double>> distances_list=new ArrayList<Tuple2<String,Double>>();
		JavaRDD<Tuple2<Double,Double>> conv_hull_rdd=sc.parallelize(convex_hull);
		conv_hull_rdd.cache();
		for(Tuple2<Double,Double> t: conv_hull_rdd.take((int)conv_hull_rdd.count())){
			for(Tuple2<Double,Double> s: conv_hull_rdd.take((int)conv_hull_rdd.count())){
				double distance;
				if(!t.equals(s)){
				distance=compute_distance(t,s);
				distances_list.add(new Tuple2<String,Double>(s.toString()+"to"+t.toString(),distance));
				}
			}
		}
		double largest=0.0D;
		String p2p="";
		for(Tuple2<String,Double> s:distances_list){
			if(s._2>largest){
				largest=s._2;
				p2p=s._1;
			}
		}
		
		
		System.out.println("The farthest points are "+p2p+" with distance "+ largest);
}
	
	public static double compute_distance(Tuple2<Double,Double> t,Tuple2<Double,Double> s){
		double distance=-100000.0D;
		distance=Math.sqrt(Math.pow(t._1-s._1,2)+Math.pow(t._2-s._2,2));
		return distance;
	}

}
