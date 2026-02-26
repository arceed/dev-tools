import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class CsvCommitInjector {
    private static final int DEFAULT_BATCH_SIZE = 5000;

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: java CsvCommitInjector <input.csv> <output.csv> [batchSize]");
            System.exit(1);
        }

        Path inputPath = Path.of(args[0]);
        Path outputPath = Path.of(args[1]);
        int batchSize = DEFAULT_BATCH_SIZE;

        if (args.length == 3) {
            try {
                batchSize = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid batchSize: " + args[2]);
                System.exit(1);
            }

            if (batchSize <= 0) {
                System.err.println("batchSize must be > 0");
                System.exit(1);
            }
        }

        try {
            injectCommitEveryBatch(inputPath, outputPath, batchSize);
            System.out.println("Done. Wrote output to: " + outputPath);
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void injectCommitEveryBatch(Path inputPath, Path outputPath, int batchSize) throws IOException {
        long lineCounter = 0;

        try (BufferedReader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {

            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();

                lineCounter++;
                if (lineCounter % batchSize == 0) {
                    writer.write("commit;");
                    writer.newLine();
                }
            }
        }
    }
}
