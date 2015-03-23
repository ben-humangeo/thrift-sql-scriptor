package com.benjamindparrish.thrift.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by bparrish on 3/22/15.
 */
public class Runner implements Runnable {

    private static final Logger _logger = LoggerFactory.getLogger(Runner.class);

    private String _filePath;
    private String _outputFilePath;
    private String _fileName;

    public Runner() { }

    public Runner setFilePath(String filePath) {
        _filePath = filePath;

        return this;
    }

    public Runner setOutputFilePath(String outputFilePath) {
        _outputFilePath = outputFilePath;

        return this;
    }

    public Runner setFileName(String fileName) {
        _fileName = fileName;

        return this;
    }

    public void run() {
        _logger.debug("filePath = {}", _filePath);
        _logger.debug("outputFilePath = {}", _outputFilePath);
        _logger.debug("fileName = {}", _fileName);

        _logger.info(String.format("starting sql-scriptor on %s", _filePath));

        ThriftParser parser = new ThriftParser(_filePath, _outputFilePath, _fileName);

        try {
            if (!parser.parse()) {
                _logger.info("failed to parse thrift definition file");

                System.exit(1);
            }
        } catch (IOException e) {
            _logger.error("error parsing thrift definition file", e);

            System.exit(1);
        }
    }

    public static void main(String... args) {
        if (args.length == 0) {
            String message = "sql-scriptor <thrift definition file path> [output path] [output file name]";

            _logger.error(message);

            System.out.println(message);

            System.exit(1);
        }

        new Runner()
                .setFilePath(args[0])
                .setOutputFilePath((args.length > 1) ? args[1] : null)
                .setFileName((args.length > 2) ? args[2] : null)
                .run();

        System.exit(0);
    }
}
