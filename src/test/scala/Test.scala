import prediction.PredictionIndex.{calculateProb, findConfusion}

import org.scalatest.flatspec.AnyFlatSpec

class SetSpec extends AnyFlatSpec {

    "row 1" should "have confusion TP" in {
        assert(findConfusion(Array(0.63, 0.98, 0.72), Array(0.36, 0.01, 0.27), "A") == "TP")
    }

    "row 11" should "have confusion TN" in {
        assert(findConfusion(Array(0.87, 0.44, 0.13), Array(0.12, 0.55, 0.86), "B") == "TN")
    }

    "row 21" should "have confusion TP" in {
        assert(findConfusion(Array(0.83, 0.61, 0.77), Array(0.16, 0.38, 0.22), "A") == "TP")
    }

    "row 31" should "have confusion FP" in {
        assert(findConfusion(Array(0.66, 0.94, 0.93), Array(0.33, 0.05, 0.06), "B") == "FP")
    }

    "row 41" should "have confusion TP" in {
        assert(findConfusion(Array(0.54, 0.88, 0.49), Array(0.45, 0.11, 0.50), "A") == "TP")
    }
    

}

