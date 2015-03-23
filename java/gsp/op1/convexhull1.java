package gsp.op1;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

class Polygon implements Serializable{
	Double x1,y1,x2,y2;
	public Polygon(Double x1,Double y1,Double x2,Double y2){
		this.x1=x1;
		this.y1=y1;
		this.x2=x2;
		this.y2=y2;
	}
	public boolean isPointWithinBoundaries(Double x,Double y){
		return ((this.x1<=x&&x<=this.x2)&&(this.y2<=y&&y<=this.y1))?true:false;
	}
	public boolean isPolygonWithinBoundaries(Double x1,Double y1,Double x2,Double y2){
		Boolean topLeft= this.isPointWithinBoundaries(x1, y1);
		Boolean bottomRight= this.isPointWithinBoundaries(x2, y2);
		return topLeft&&bottomRight;
	}
}
class Points implements Serializable{
	Double x1,y1;
	public Points(Double x1,Double y1){
		this.x1=x1;
		this.y1=y1;
	}
}

public class convexhull1 {
	public static ArrayList<Points> calc_conv_hull(Points p){
		ArrayList<Points> hull=new ArrayList<Points>();
		System.out.println(p.x1+","+p.y1);
		return hull;
	} 
	private class TupleComparator implements Comparator<Tuple2<Double, Double>>, Serializable {
	    public int compare(Tuple2<Double, Double> tuple1, Tuple2<Double, Double> tuple2) {
	        return tuple1._1 <= tuple2._1 && tuple1._2<=tuple2._2? 0 : 1;
	    }
	}
	public static void main(String args[]){
		SparkConf conf= new SparkConf().setAppName("c_hull").setMaster("local[2]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> lines=sc.textFile("C:/testdata/ConvexHullTestData.csv",1);
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
		
		for(Tuple2<Double,Double> t:l)
		{
			//System.out.println(t);
		}
		List<Tuple2<Double,Double>> convex_hull=new ArrayList<Tuple2<Double,Double>>();
		List<Tuple2<Double,Double>> leftSet=new ArrayList<Tuple2<Double,Double>>();
		List<Tuple2<Double,Double>> rightSet=new ArrayList<Tuple2<Double,Double>>();
		
		compute_hull(l,convex_hull,leftSet,rightSet);
		for(Tuple2<Double,Double> t:convex_hull)
        	System.out.println(t._1 + " "+ t._2);
}
	
	public static void compute_hull(List<Tuple2<Double,Double>> l,List<Tuple2<Double,Double>> convex_hull,List<Tuple2<Double,Double>> rightSet,List<Tuple2<Double,Double>> leftSet){
		Tuple2<Double,Double> min_x=l.get(0);
		Tuple2<Double,Double> max_x=l.get(l.size()-1);
		convex_hull.add(min_x);
		convex_hull.add(max_x);
		
		l.remove(min_x);
		l.remove(max_x);
        for (int i = 0; i < l.size(); i++)
        {
            Tuple2<Double,Double> temp = l.get(i);
            if (pointLocation(min_x, max_x, temp) == -1)
                leftSet.add(temp);
            else if (pointLocation(min_x, max_x, temp) == 1)
                rightSet.add(temp);
        }
        hullset(min_x, max_x, rightSet, convex_hull);
        hullset(max_x, min_x, leftSet, convex_hull);
	}
	
    public static double distance(Tuple2<Double,Double> A, Tuple2<Double,Double> B, Tuple2<Double,Double> C)
    {

        double ABx = B._1 - A._1;
        double ABy = B._2 - A._2;
        double num = ABx * (A._2 - C._2) - ABy * (A._1 - C._1);
        if (num < 0)
            num = -num;
        return num;
    }
    
	public static void hullset(Tuple2<Double,Double> A,Tuple2<Double,Double> B,List<Tuple2<Double,Double>> set,List<Tuple2<Double,Double>> hull){
        int insertPosition = hull.indexOf(B);
        if (set.size() == 0)
            return;
        if (set.size() == 1)
        {
        	Tuple2<Double,Double> p = set.get(0);
            set.remove(p);
            hull.add(insertPosition, p);
            return;
        }

        double dist = Integer.MIN_VALUE;
        int furthestPoint = -1;
        for (int i = 0; i < set.size(); i++)
        {
        	Tuple2<Double,Double> p = set.get(i);
            double distance = distance(A, B, p);
            if (distance > dist)
            {
                dist = distance;
                furthestPoint = i;
            }
        }

        Tuple2<Double,Double> P = set.get(furthestPoint);
        set.remove(furthestPoint);
        hull.add(insertPosition, P);
        // Determine who's to the left of AP
        List<Tuple2<Double,Double>> leftSetAP = new ArrayList<Tuple2<Double,Double>>();

        for (int i = 0; i < set.size(); i++)
        {
        	Tuple2<Double,Double> M = set.get(i);
            if (pointLocation(A, P, M) == 1)
            {
                leftSetAP.add(M);
            }

        }

        // Determine who's to the left of PB
        List<Tuple2<Double,Double>> leftSetPB = new ArrayList<Tuple2<Double,Double>>();

        for (int i = 0; i < set.size(); i++)
        {
        	Tuple2<Double,Double> M = set.get(i);
            if (pointLocation(P, B, M) == 1)
            {
                leftSetPB.add(M);
            }
        }
        hullset(A, P, leftSetAP, hull);
        hullset(P, B, leftSetPB, hull);		
	}
	
	public static int pointLocation(Tuple2<Double,Double> A,Tuple2<Double,Double> B,Tuple2<Double,Double> P){
        double cp1 = (B._1 - A._1) * (P._2 - A._2) - (B._2 - A._2) * (P._1 - A._1);
        if (cp1 > 0)
            return 1;
        else if (cp1 == 0)
            return 0;
        else
            return -1;
		
	}
}