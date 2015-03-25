package gsp.op1;

import static java.lang.Math.*;
import math.geom2d.Box2D;
import math.geom2d.Point2D;
import math.geom2d.circulinear.CirculinearDomain2D;
import math.geom2d.circulinear.buffer.BufferCalculator;
import math.geom2d.domain.Boundary2D;
import math.geom2d.domain.Boundaries2D;
import math.geom2d.domain.Contour2D;
import math.geom2d.domain.ContourArray2D;
import math.geom2d.point.PointSets2D; 
import math.geom2d.polygon.LinearRing2D;
import math.geom2d.polygon.Polygon2D;
import math.geom2d.polygon.SimplePolygon2D;

import com.seisw.util.geom.Poly; 
import com.seisw.util.geom.PolyDefault; 
import com.seisw.util.geom.PolySimple; 

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

public class Union implements Serializable {
	
	public static void main(String[] args){
	SparkConf conf= new SparkConf().setAppName("union").setMaster("local[2]");
	JavaSparkContext sc=new JavaSparkContext(conf);
	JavaRDD<String> lines=sc.textFile("C:/testdata/PolygonUnionTestData.csv",1);
	
	JavaPairRDD<Points,Points> points=lines.mapToPair(new PairFunction<String, Points,Points>(){
		public Tuple2<Points,Points> call(String s){
			
			if(!s.equals("")&&!s.contains("x")){
			String[] s1=s.split(",");
			return new Tuple2<Points,Points>(new Points(Double.parseDouble(s1[0]),Double.parseDouble(s1[1])),new Points(Double.parseDouble(s1[2]),Double.parseDouble(s1[3])));
			}
			else
				return new Tuple2<Points,Points>(new Points(0D,0D),new Points(0D,0D));
		}
	}).filter(new Function<Tuple2<Points,Points>,Boolean>(){
		public Boolean call(Tuple2<Points,Points> t){
			if(! (t._1.x1==0))
			return true;
			return false;
		}
		
	});

	for(Tuple2<Points,Points> t: points.collect()){
	Polygon2D p=Polygons2D.CreatePolygon(t._1.x1, t._1.y1, t._2.x1, t._2.y1);
	System.out.println(p.vertices());
	}
	
	JavaRDD<Polygon> polys=points.map(new Function<Tuple2<Points,Points>,Polygon>(){
		public Polygon call(Tuple2<Points,Points> t){
			return new Polygon(t._1.x1, t._1.y1, t._2.x1, t._2.y1);
		}
	});
	
	for(Polygon p:polys.collect())
		for(Points x:p.vertices)
		System.out.println(x.x1+" "+x.y1);
	
	try{
	Polygon union=polys.reduce(new Function2<Polygon,Polygon,Polygon>(){
		public Polygon call(Polygon q,Polygon o){
			 Polygon2D p=new SimplePolygon2D();
			for(Points vertex:q.vertices)
			p.addVertex(new Point2D(vertex.x1,vertex.y1));
			
			Polygon2D l=new SimplePolygon2D();
			for(Points vertex:o.vertices)
			l.addVertex(new Point2D(vertex.x1,vertex.y1));
			
			Polygon2D unioned=Polygons2D.union(p,l);
			q=new Polygon();
			
			for(Point2D x:unioned.vertices())
			q.vertices.add(new Points(x.getX(),x.getY()));
			return q;
		}
	});
	
	for(Points p:union.vertices){
		System.out.println(p.x1+" "+p.y1);
	}
	
	}
	catch(Exception e){System.out.println(e.getCause());
	System.out.println(e.getClass());
	System.out.println(e.getLocalizedMessage());
	//System.out.println(e.getMessage());
	}
	}
}