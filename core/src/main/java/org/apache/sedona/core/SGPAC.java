package org.apache.sedona.core;

import org.apache.sedona.core.formatMapper.WkbReader;
import org.apache.sedona.core.formatMapper.WktReader;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.*;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;


//Longitudes range from -180 to 180.
//Latitudes range from -90 to 90

public class SGPAC {

    private static final String APP_NAME = "SGPAC";

    private static void writeToFile(String output_path, String data) throws IOException {
        FileWriter outputFileWriter = new FileWriter(output_path, true);
        PrintWriter outputPrintWriter = new PrintWriter(outputFileWriter);
        outputPrintWriter.println(data);
        outputPrintWriter.close();
    }

    public static void main(String[] args) {

        Date date = new Date();
        SimpleDateFormat dateFormatter = new SimpleDateFormat("dd-MM-yyyy@HH:mm:ss");
        String applicationName = APP_NAME + dateFormatter.format(date);

        SparkConf conf = new SparkConf().setAppName(applicationName);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        Options options = new Options();

        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option query = new Option("q", "query", true, "query file path");
        query.setRequired(true);
        options.addOption(query);

        Option output = new Option("o", "output", true, "output file path");
        output.setRequired(true);
        options.addOption(output);

        Option xGridCells = new Option("x", "xgridcell", true, "number of local x-axis grid cells");
        xGridCells.setRequired(false);
        options.addOption(xGridCells);

        Option yGridCells = new Option("y", "ygridcell", true, "number of local y-axis grid cells");
        yGridCells.setRequired(false);
        options.addOption(yGridCells);

        Option localcapacity = new Option("l", "localcapacity", true, "maximum node capacity for local index");
        localcapacity.setRequired(false);
        options.addOption(localcapacity);

        Option nodecapacity = new Option("c", "nodecapacity", true, "partitioning node capacity");
        nodecapacity.setRequired(true);
        options.addOption(nodecapacity);

        Option help = new Option("h", "help", false, "print command line options");
        help.setRequired(false);
        options.addOption(help);


        try {
            cmd = parser.parse(options, args);

            if(cmd.hasOption("help")) {
                formatter.printHelp("utility-name", options);
                System.exit(0);
            }

            String inputFilePath = cmd.getOptionValue("input");
            String outputFilePath = cmd.getOptionValue("output");
            String queryFilePath = cmd.getOptionValue("query");

            int capacity = Integer.parseInt(cmd.getOptionValue("nodecapacity")) * 1000;

            SpatialRDD spatialRDD = new PointRDD(sparkContext, inputFilePath, 0, FileDataSplitter.CSV, true);
            spatialRDD.analyze();
            spatialRDD.quadSpatialPartitioning(capacity);

            int numXGridCells = 0;
            int numYGridCells = 0;
            int localNodeCapacity = 0;

            if(cmd.hasOption("localcapacity")) {
                localNodeCapacity = Integer.parseInt(cmd.getOptionValue("localcapacity"));
                spatialRDD.buildRIndex(spatialRDD.getGlobalIndexGrids(), localNodeCapacity, true);
            } else if(cmd.hasOption("xgridcell") && cmd.hasOption("ygridcell")) {
                numXGridCells = Integer.parseInt(cmd.getOptionValue("xgridcell"));
                numYGridCells = Integer.parseInt(cmd.getOptionValue("ygridcell"));
                spatialRDD.buildGridIndex(spatialRDD.getGlobalIndexGrids(), numXGridCells, numYGridCells, true);
            }
            spatialRDD.rawSpatialRDD.unpersist();
            spatialRDD.spatialPartitionedRDD.unpersist();

            SpatialRDD queryRDD = WktReader.readToGeometryRDD(sparkContext, queryFilePath, 0, false, true);
            queryRDD.rawSpatialRDD = queryRDD.rawSpatialRDD.filter(Objects::nonNull);
            queryRDD.spatialPartitionedRDD = queryRDD.createGlobalIndex(spatialRDD.getGlobalIndex());

            writeToFile(outputFilePath, "Number of indexes: " + spatialRDD.indexedPairRDD.count());
            writeToFile(outputFilePath, "Number of query polygons: " + queryRDD.spatialPartitionedRDD.count());

            String[] temp = queryFilePath.split("/");
            String layerName = temp[temp.length - 1];

            long start_time = System.nanoTime();
            double duration;

//            JavaPairRDD result1 = RangeQuery.indexQueryPolygon(spatialRDD, queryRDD);
//            Map mapResult1 = result1.collectAsMap();
//
//            duration = (System.nanoTime() - start_time)/1e+9;
//            duration /= 60;
//            writeToFile(outputFilePath, "Oracle Index Query Polygon Method: ");
//            writeToFile(outputFilePath, layerName + " result size: " + mapResult1.size());
//            writeToFile(outputFilePath, layerName + ": " + mapResult1);
//            writeToFile(outputFilePath, layerName + " total time: " + duration + " minutes");
//            writeToFile(outputFilePath, "---------------------------------------------------------------------");

            start_time = System.nanoTime();

            JavaPairRDD result2 = RangeQuery.SpatialJoinDecomposition(spatialRDD, queryRDD);
            Map mapResult2 = result2.collectAsMap();

            duration = (System.nanoTime() - start_time)/1e+9;
            duration /= 60;

            writeToFile(outputFilePath, "SGPAC Decomposition Method: ");
            writeToFile(outputFilePath, layerName + " result size: " + mapResult2.size());
            writeToFile(outputFilePath, layerName + ": " + mapResult2);
            writeToFile(outputFilePath, layerName + " total time: " + duration + " minutes");
            writeToFile(outputFilePath, "---------------------------------------------------------------------");

            start_time = System.nanoTime();

            JavaPairRDD result3 = RangeQuery.SpatialJoinFilterRefine(spatialRDD, queryRDD, false);
            Map mapResult3 = result3.collectAsMap();

            duration = (System.nanoTime() - start_time)/1e+9;
            duration /= 60;

            writeToFile(outputFilePath, "Original Filter-refine Method: ");
            writeToFile(outputFilePath, layerName + " result size: " + mapResult3.size());
            writeToFile(outputFilePath, layerName + ": " + mapResult3);
            writeToFile(outputFilePath, layerName + " total time: " + duration + " minutes");
            writeToFile(outputFilePath, "---------------------------------------------------------------------");


        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Test failed: " + e.getMessage());
        }

    }

}
