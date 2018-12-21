package sparksqljava.teacher;

import java.util.Comparator;

public class StringComparator implements Comparator {

	@Override
	public int compare(Object o1, Object o2) {
		String name1=(String)o1;
		String name2=(String)o2;
		return name1.compareTo(name2);
	}

}
