package server.dsmr.impl;

import server.DFS;
import server.dsmr.MapInput;
import server.dsmr.MapReduce;
import server.dsmr.ReduceInput;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

public class KMeansClustering extends MapReduce {
    private static final String CENTROIDS_FILE = "kmeans-centroid";
    private final DFS DFS;

    public KMeansClustering(BiConsumer<String, String> Emit, DFS DFS) {
        // Define emitter
        super(Emit);
        this.DFS = DFS;
    }

    private class Point {
        double x;
        double y;

        Point(String str) {
            String[] split = str.split(",");
            x = Double.parseDouble(split[0]);
            y = Double.parseDouble(split[1]);
        }

        Point (double x, double y) {
            this.x = x;
            this.y = y;
        }

        double squareDist(Point p) {
            double dx, dy;
            dx = p.x - x;
            dy = p.y - y;
            return dx*dx + dy*dy;
        }

        String str() {
            return String.format("%.4f,%.4f",x,y);
        }
    }

    @Override
    public void Map(MapInput input) {
        List<Point> centroidPoints = new ArrayList<>();
        List<Point> dataPoints = new ArrayList<>();
        try {
            String[] split = DFS.get(CENTROIDS_FILE).split(" ");
            for (String str : split) {
                centroidPoints.add(new Point(str));
            }

            split = input.getValue().split(" ");
            for (String str : split) {
                dataPoints.add(new Point(str));
            }
        } catch (Exception e) {
            return;
        }

        for (Point x_i : dataPoints) {
            Integer clusterId = -1;
            double minDist = Double.MAX_VALUE;
            double dist;
            int j=0;
            for (Point c_j : centroidPoints) {
                dist = x_i.squareDist(c_j);
                if (dist < minDist) {
                    minDist = dist;
                    clusterId = j;
                }

                j++;
            }

            Emit.accept(clusterId.toString(), x_i.str());
        }
    }

    @Override
    public void Reduce(ReduceInput input) {
        double xTotal=0, yTotal=0;

        Iterator<String> dataPoints = input.iterator();
        Point x_i;
        int i=0;
        while (dataPoints.hasNext()) {
            x_i = new Point(dataPoints.next());
            xTotal += x_i.x;
            yTotal += x_i.y;

            i++;
        }

        Point c = new Point(xTotal/i, yTotal/i);
        Emit.accept(input.getKey(), c.str());
    }
}
