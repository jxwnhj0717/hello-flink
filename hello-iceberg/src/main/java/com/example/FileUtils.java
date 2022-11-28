package com.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Scanner;

public class FileUtils {
    public static String readFile(String name) throws FileNotFoundException, URISyntaxException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(name);
        Scanner scanner = new Scanner(new File(url.toURI()));
        scanner.useDelimiter("\\Z");
        String text = scanner.next();
        return text;
    }
}
