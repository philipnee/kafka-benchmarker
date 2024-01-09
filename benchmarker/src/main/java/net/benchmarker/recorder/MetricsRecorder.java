package net.benchmarker.recorder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import net.benchmarker.metrics.RunMetrics;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MetricsRecorder implements Recorder {
    private final String filePath;
    private final Gson gson;

    public MetricsRecorder(final Optional<String> filePath) {
        if (filePath.isPresent()) {
            this.filePath = filePath.get();
            initializeFile();
        } else {
            this.filePath = "screen";
        }
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    private void initializeFile() {
        // Check if the file exists, if not, create a new one with an empty JSON array
        if (!Files.exists(Paths.get(filePath))) {
            try {
                FileWriter writer = new FileWriter(filePath);
                writer.write("[]");
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private List<RunMetrics> readRunResults() {
        try {
            Reader reader = new FileReader(filePath);
            Type listType = new TypeToken<ArrayList<RunMetrics>>(){}.getType();
            return gson.fromJson(reader, listType);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    private void writeRunResults(RunMetrics runResults) {
        try {
            FileWriter writer = new FileWriter(filePath, true);
            gson.toJson(runResults, writer);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void record(RunMetrics metrics) {
        if (filePath.equals("screen")) {
            System.out.println(gson.toJson(metrics));
        } else {
            writeRunResults(metrics);
        }
    }
}
