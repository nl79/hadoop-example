import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Apriori {

  public static boolean hasMinSupport(double minSupPercent, int maxNumTxns, int itemCount) {
    boolean hasMinSupport = false;
    int minSupport = (int)((double)(minSupPercent * maxNumTxns))/100;
    if(itemCount >= minSupport) {
      hasMinSupport = true;
    }

    return hasMinSupport;
  }

  public static boolean hasSupport(double support, int total,  int count) {
    double result = (double)count / (double)total;
    return result >= support;
  }

  public static List<Set> getCandidateSets(List<Set> largeSetPrevPass, int SetSize)
  {
    List<Set> candidateSets = new ArrayList<Set>();
    List<Integer> newItems = null;
    Map<Integer, List<Set>> largeSetMap = getLargeSetMap(largeSetPrevPass);
    Collections.sort(largeSetPrevPass);

    for(int i=0; i < (largeSetPrevPass.size() -1); i++) {
      for(int j=i+1; j < largeSetPrevPass.size() ; j++) {
        List<Integer> outerItems = largeSetPrevPass.get(i).getItems();
        List<Integer> innerItems = largeSetPrevPass.get(j).getItems();

        if((SetSize - 1) > 0) {
          boolean isMatch = true;
          for(int k=0; k < (SetSize -1); k++) {
            if(!outerItems.get(k).equals(innerItems.get(k))) {
              isMatch = false;
              break;
            }
          }


          if(isMatch) {
            newItems = new ArrayList<Integer>();
            newItems.addAll(outerItems);
            newItems.add(innerItems.get(SetSize-1));

            Set newSet = new Set(newItems, 0);
            if(prune(largeSetMap, newSet)) {
              candidateSets.add(newSet);
            }
          }
        }
        else {
          if(outerItems.get(0) < innerItems.get(0)) {
            newItems = new ArrayList<Integer>();
            newItems.add(outerItems.get(0));
            newItems.add(innerItems.get(0));

            Set newSet = new Set(newItems, 0);

            candidateSets.add(newSet);
          }
        }
      }
    }
    return candidateSets;
  }

  /*
   * Generates a map of hashcode and the corresponding Set. Since multiple entries can
   * have the same hashcode, there would be a list of Sets for any hashcode.
   */
  public static Map<Integer, List<Set>> getLargeSetMap(List<Set> largeSets)
  {
    Map<Integer, List<Set>> largeSetMap = new HashMap<Integer, List<Set>>();

    List<Set> Sets = null;
    for(Set largeSet : largeSets) {
      int hashCode = largeSet.hashCode();
      if(largeSetMap.containsKey(hashCode)) {
        Sets = largeSetMap.get(hashCode);
      }
      else {
        Sets = new ArrayList<Set>();
      }

      Sets.add(largeSet);
      largeSetMap.put(hashCode, Sets);
    }

    return largeSetMap;
  }

  private static boolean prune(Map<Integer, List<Set>> largeSetsMap, Set newSet)
  {
    List<Set> subsets = getSubsets(newSet);

    for(Set s : subsets) {
      boolean contains = false;
      int hashCodeToSearch = s.hashCode();
      if(largeSetsMap.containsKey(hashCodeToSearch)) {
        List<Set> candidateSets = largeSetsMap.get(hashCodeToSearch);
        for(Set Set : candidateSets) {
          if(Set.equals(s)) {
            contains = true;
            break;
          }
        }
      }

      if(!contains)
        return false;
    }

    return true;
  }

  /*
   * Generate all possible k-1 subsets for this Set (preserves order)
   */
  private static List<Set> getSubsets(Set Set)
  {
    List<Set> subsets = new ArrayList<Set>();

    List<Integer> items = Set.getItems();
    for(int i = 0; i < items.size(); i++) {
      List<Integer> currItems = new ArrayList<Integer>(items);
      currItems.remove(items.size() - 1 - i);
      subsets.add(new Set(currItems, 0));
    }

    return subsets;
  }
}