package io.bsrevanth2011.github.graveldb.server;

import org.eclipse.collections.api.factory.Lists;

import java.util.List;

public class Log {
    List<Entry> log = Lists.mutable.empty();

    public Entry get(int index) {
        return log.get(index);
    }

    public void add(int term, Command command) {
        log.add(new Entry(term, command));
    }

}

record Entry(int term, Command command) { }

record Command(String op, Object value) { }
