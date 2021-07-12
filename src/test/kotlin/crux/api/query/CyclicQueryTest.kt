package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.query.conversion.q
import crux.api.tx.submitTx
import crux.api.underware.kw
import crux.api.underware.sym
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.singleResultSet
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CyclicQueryTest {
    companion object {
        private val aNodes = (1..4).map { index ->
            CruxDocument.build("a-$index") {
                it.put(
                    "next",
                    if (index == 4) "a-1" else "a-${index+1}"
                )
            }
        }

        private val bNodes = (1..5).map { index ->
            CruxDocument.build("b-$index") {
                it.put(
                    "next",
                    if (index == 5) "b-1" else "b-${index+1}"
                )
            }
        }

        private val start = "start".sym
        private val end = "end".sym
        private val node = "node".sym
        private val intermediate = "intermediate".sym

        private val key = "crux.db/id".kw
        private val next = "next".kw

        private val pointsTo = "pointsTo".sym
        private val points2 = "points2".sym
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            aNodes.forEach(::put)
            bNodes.forEach(::put)
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `can find all nodes in cycle`() {
        assertThat(
            db.q {
                find {
                    + node
                }

                where {
                    start has key eq "a-1"
                    end has key eq "a-1"
                    rule(pointsTo) (start, node)
                    rule(pointsTo) (node, end)
                }

                rules {
                    def(pointsTo) (start, end) {
                        start has next eq end
                    }

                    def(pointsTo) (start, end) {
                        start has next eq intermediate
                        rule(pointsTo) (intermediate, end)
                    }
                }
            }.singleResultSet(),
            equalTo(
                aNodes.map(CruxDocument::getId).toSet()
            )
        )
    }
}