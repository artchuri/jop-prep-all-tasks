package nip.edu.jobprep.repositories;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class UserRepository {
    private final DSLContext dsl;

    public UserRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

}
