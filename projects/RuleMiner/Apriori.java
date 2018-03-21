import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Apriori {
  public static boolean hasSupport(double support, int total,  int count) {
    double result = Apriori.support(count, total);

    return result >= support;
  }

  public static double support(int count, int total) {
    return (double)count / (double)total;
  }
}