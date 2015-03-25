package gsp.op1;

import static java.lang.Math.*;

import java.awt.Graphics2D;
import java.awt.Polygon;

import math.geom2d.Box2D;
import math.geom2d.Point2D;
import math.geom2d.circulinear.CirculinearDomain2D;
import math.geom2d.circulinear.buffer.BufferCalculator;
import math.geom2d.domain.Boundary2D;
import math.geom2d.domain.Boundaries2D;
import math.geom2d.domain.Contour2D;
import math.geom2d.domain.ContourArray2D;
import math.geom2d.point.PointSets2D; 
import math.geom2d.polygon.*;

import com.seisw.util.geom.Poly; 
import com.seisw.util.geom.PolyDefault; 
import com.seisw.util.geom.PolySimple;

import java.awt.Graphics;
import java.io.Serializable;

import javax.swing.JComponent;
import javax.swing.JFrame;

public final class Polygons2D implements Serializable {
	private final static PolySimple convertToGpcjSimplePolygon(LinearRing2D ring) {
			    	PolySimple poly = new PolySimple();
			    	for (Point2D point : ring.vertices())
			    		poly.add(new com.seisw.util.geom.Point2D(point.x(), point.y()));

			    	return poly;
			    }	
	
	private final static Poly convertToGpcjPolygon(Polygon2D polygon) {
    	PolyDefault result = new PolyDefault();
    	for (LinearRing2D ring : polygon.contours())
    		result.add(convertToGpcjSimplePolygon(ring));
    	return result;
    }

	private final static LinearRing2D convertFromGpcjSimplePolygon(
    		Poly poly) {
    	return LinearRing2D.create(extractPolyVertices(poly));
    }
	  private final static Point2D[] extractPolyVertices(Poly poly) {
	    	int n = poly.getNumPoints();
	    	Point2D[] points = new Point2D[n];
	    	for (int i = 0; i < n; i++)
	    		points[i] = new Point2D(poly.getX(i), poly.getY(i));
	    	return points;
	    }

	
	private final static Polygon2D convertFromGpcjPolygon(Poly poly) {
    	int n = poly.getNumInnerPoly();
    	
    	// if the result is single, create a SimplePolygon
    	if (n == 1) {
    		Point2D[] points = extractPolyVertices(poly.getInnerPoly(0));
    		return SimplePolygon2D.create(points);
    	}
    	
    	// extract the different rings of the resulting polygon
    	LinearRing2D[] rings = new LinearRing2D[n];
    	for (int i = 0; i < n; i++) 
    		rings[i] = convertFromGpcjSimplePolygon(poly.getInnerPoly(i));
    	
    	// create a multiple polygon
    	return MultiPolygon2D.create(rings);
    }
    

public final static Polygon2D union(Polygon2D polygon1, Polygon2D polygon2) {

   	Poly poly1 = convertToGpcjPolygon(polygon1);

   	Poly poly2 = convertToGpcjPolygon(polygon2);

   	Poly result = poly1.union(poly2);
   	return convertFromGpcjPolygon(result);

   }

public static void main(String[] args){
	Polygon2D p1=new SimplePolygon2D();
	Polygon2D p2=new SimplePolygon2D();
	Polygon2D p3=new SimplePolygon2D();
	Polygon2D p4=new SimplePolygon2D();
	Polygon2D p5=new SimplePolygon2D();
	Polygon2D p6=Polygons2D.createRec
	
	/*polygon1.addVertex(new Point2D(3,1));
	polygon1.addVertex(new Point2D(4,1));
	polygon1.addVertex(new Point2D(4,2));
	polygon1.addVertex(new Point2D(3,2));
	//polygon1.addVertex(new Point2D(4,1));
	
	polygon1.addVertex(new Point2D(3,1));
	polygon2.addVertex(new Point2D(3,2));
	polygon2.addVertex(new Point2D(2,2));
	polygon2.addVertex(new Point2D(2,1));
	//polygon2.addVertex(new Point2D(3,1));
	*/
/*	Point2D px=new Point2D(0.321534855,0.036295831);
	Point2D pyx=new Point2D(-0.23567288,0.036295831);
	Point2D py=new Point2D(-0.23567288,-0.415640992);
	Point2D pxx=new Point2D(0.321534855,-0.415640992);
	
	Point2D px1=new Point2D(0.115064798,0.105952147);
	Point2D pyx1=new Point2D(-0.161920957,0.105952147);
	Point2D py1=new Point2D(-0.161920957,-0.405533972);
	Point2D pxx1=new Point2D(0.115064798,-0.405533972);
	//0.115064798,0.105952147,-0.161920957,-0.405533972
	
	Point2D px2=new Point2D(0.238709092,0.016298271);
	Point2D pyx2=new Point2D(-0.331934184,0.01629827);
	Point2D py2=new Point2D(-0.331934184,-0.18218141);
	Point2D pxx2=new Point2D(0.238709092,-0.18218141);
	//0.238709092,0.016298271,-0.331934184,-0.18218141
	
	Point2D px3=new Point2D(0.2069243,0.223297076);
	Point2D pyx3=new Point2D(-0.050542958,0.223297076);
	Point2D py3=new Point2D(-0.050542958,-0.475492946);
	Point2D pxx3=new Point2D(0.2069243,-0.475492946);
	//0.2069243,0.223297076,-0.050542958,-0.475492946
	
	Point2D px4=new Point2D(0.321534855,0.036295831);
	Point2D pyx4=new Point2D(-0.440428957,0.036295831);
	Point2D py4=new Point2D(-0.440428957,-0.289485599);
	Point2D pxx4=new Point2D(0.321534855,-0.289485599);
	//0.321534855,0.036295831,-0.440428957,-0.289485599
	
	
	polygon1.addVertex(pxx);
	polygon1.addVertex(pyx);
	polygon1.addVertex(px);
	polygon1.addVertex(py);
	

	polygon2.addVertex(pxx1);
	polygon2.addVertex(pyx1);
	polygon2.addVertex(px1);
	polygon2.addVertex(py1);
	
	polygon3.addVertex(pxx2);
	polygon3.addVertex(pyx2);
	polygon3.addVertex(px2);
	polygon3.addVertex(py2);
	
	polygon4.addVertex(pxx3);
	polygon4.addVertex(pyx3);
	polygon4.addVertex(px3);
	polygon4.addVertex(py3);
	
	polygon5.addVertex(pxx4);
	polygon5.addVertex(pyx4);
	polygon5.addVertex(px4);
	polygon5.addVertex(py4);
	*/
	
	/*p1=CreatePolygon(0.321534855,0.036295831,-0.23567288,-0.415640992);
	p2=CreatePolygon(0.115064798,0.105952147,-0.161920957,-0.405533972);
	p3=CreatePolygon(0.238709092,0.016298271,-0.331934184,-0.18218141);
	p4=CreatePolygon(0.2069243,0.223297076,-0.050542958,-0.475492946);
	p5=CreatePolygon(0.321534855,0.036295831,-0.440428957,-0.289485599);
*/
/*	p1=CreatePolygon(2,2,4,-3);
	p2=CreatePolygon(1,-1,5,-5);
	p3=CreatePolygon(1,-2,4.5,-4);
	//p4=CreatePolygon(0.2069243,0.223297076,-0.050542958,-0.475492946);
	
	Polygon2D p11=union(p1, p2);
	p11=union (p11,p3);
	//Polygon2D p22=union (p4,p5);
	//Polygon2D p33=union (p11,p22);
	
	*/
	
	//p1=CreatePolygon(0.321534855,0.036295831,-0.23567288,-0.415640992);
	//p2=CreatePolygon(0.115064798,0.105952147,-0.161920957,-0.405533972);
	//p3=CreatePolygon(0.238709092,0.016298271,-0.331934184,-0.18218141);
	//p4=CreatePolygon(0.2069243,0.223297076,-0.050542958,-0.475492946);
	//p5=CreatePolygon(0.321534855,0.036295831,-0.440428957,-0.289485599);
	
	/*printcoords(0.321534855,0.036295831,-0.23567288,-0.415640992);
	printcoords(0.115064798,0.105952147,-0.161920957,-0.405533972);
	printcoords(0.238709092,0.016298271,-0.331934184,-0.18218141);
	printcoords(0.2069243,0.223297076,-0.050542958,-0.475492946);
	printcoords(0.321534855,0.036295831,-0.440428957,-0.289485599);
	*/

	//Polygon2D p11=union (p1,p5);
	//p11=union (p11,p3);
	//p11=union (p11,p4);
	//p11=union(p11, p5);
	
	//Polygon2D p22=union (p4,p5);
	//Polygon2D p33=union (p11,p22);
	
	//for(Point2D p: p11.vertices())
		//System.out.println(p);
	
	
}

public static void printcoords(double x1,double y1,double x2,double y2){
	System.out.println(x1);
	System.out.println(x2);
	System.out.println(x2);
	System.out.println(x1);
	
}
public static Polygon2D CreatePolygon(double x1,double y1,double x2,double y2){
	Polygon2D p=new SimplePolygon2D();
	p.addVertex(new Point2D(x1,y1));
	p.addVertex(new Point2D(x1,y2));
	p.addVertex(new Point2D(x2,y2));
	p.addVertex(new Point2D(x2,y1));
	return p;
}

}