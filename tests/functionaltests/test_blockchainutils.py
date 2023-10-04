import os
import sys

module_path = os.path.dirname(os.path.abspath(__file__))
print(module_path)
sys.path.append(os.path.join(module_path, "../../"))

import unittest

from yadacoin.core.blockchainutils import BlockChainUtils


class TestBlockchainUtilities(unittest.TestCase):
    async def test_is_input_spent(self):
        self.assertTrue(
            await BlockChainUtils().is_input_spent(
                "MEQCID7EJG34qodpxpsyhjUr3YDXVYw6T8VgzVOzSs3bYTxNAiAXGSM1NzA/g43pa7u1yckQuiaLYLilnUQWEPNfhyFS7w==",
                "02fa9550f57055c96c7ce4c6c9cd1411856beba5c7d5a07417e980a39aa03da3dc",
            )
        )
        self.assertFalse(
            await BlockChainUtils().is_input_spent(
                "MMMMEQCID7EJG34qodpxpsyhjUr3YDXVYw6T8VgzVOzSs3bYTxNAiAXGSM1NzA/g43pa7u1yckQuiaLYLilnUQWEPNfhyFS7w==",  # signature that will never exist
                "02fa9550f57055c96c7ce4c6c9cd1411856beba5c7d5a07417e980a39aa03da3dd",  # public_key that will never exist
            )
        )


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
