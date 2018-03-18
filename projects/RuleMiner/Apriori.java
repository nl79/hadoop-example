import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Apriori {
  public static boolean hasSupport(double support, int total,  int count) {
    double result = (double)count / (double)total;

    return result >= support;
  }
}