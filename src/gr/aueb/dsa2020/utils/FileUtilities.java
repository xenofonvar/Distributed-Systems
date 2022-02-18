package gr.aueb.dsa2020.utils;

import java.util.Optional;

public class FileUtilities {
    // returns the extension file underlined from String filename (absolute or relative path of the file)
    // if this function fails top retrieve the extension of a file then returns null
    public static String getExtensionOf(String filename) {
        Optional<String> extension = Optional.ofNullable(filename)
                                        .filter(f -> f.contains("."))
                                        .map(f -> f.substring(filename.lastIndexOf(".") + 1));
        return extension.orElse(null);
    }
}
