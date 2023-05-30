package io.bsrevanth2011.github.graveldb.log;

import lombok.Getter;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;

@Getter
@Setter
public class LogMetaData implements Serializable {
    @Serial private static final long serialVersionUID = 4123423L;
    private int lastLogIndex = 0;
    private int lastLogTerm = 0;
}
