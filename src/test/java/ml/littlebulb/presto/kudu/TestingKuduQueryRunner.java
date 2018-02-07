/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.littlebulb.presto.kudu;

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;

import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.util.Locale.ENGLISH;

public class TestingKuduQueryRunner
        extends DistributedQueryRunner {
    public static final String TESTING_SCHEMA = "testing";

    private TestingKuduQueryRunner(Session session, int workers)
            throws Exception {
        super(session, workers);
    }

    public static TestingKuduQueryRunner createKuduQueryRunner()
            throws Exception {
        TestingKuduQueryRunner queryRunner = null;
        try {
            queryRunner = new TestingKuduQueryRunner(createSession(), 3);

            Map<String, String> properties = ImmutableMap.of(
                    "kudu.client.master-addresses", "localhost:7051");

            queryRunner.installPlugin(new KuduPlugin());
            queryRunner.createCatalog("kudu", "kudu", properties);

            queryRunner.execute(createSession(), "DROP SCHEMA IF EXISTS testing");
            queryRunner.execute(createSession(), "CREATE SCHEMA testing");

            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession() {
        return testSessionBuilder()
                .setCatalog("kudu")
                .setSchema(TESTING_SCHEMA)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }

    public void shutdown() {
        close();
    }
}
