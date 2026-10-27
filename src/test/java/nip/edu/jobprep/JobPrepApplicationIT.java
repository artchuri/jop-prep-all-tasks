package nip.edu.jobprep;

import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class JobPrepApplicationIT extends AbstractIntegrationTest {

    @Test
    void contextLoadsWithRealPostgres() {
        assertThat(POSTGRES.isRunning()).isTrue();
    }


}
