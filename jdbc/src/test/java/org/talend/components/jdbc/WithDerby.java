package org.talend.components.jdbc;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

@Target(TYPE)
@Retention(RUNTIME)
@ExtendWith(DerbyExtension.class)
public @interface WithDerby {

    String server() default "localhost";

    int port() default 0;

    String dbName() default "TestDB";

    String user() default "sa";

    String password() default "sa";

    boolean createDb() default true;

    String onStartSQLScript() default "";

    String onShutdownSQLScript() default "";

    String logFile() default "target/derby.log";

}
