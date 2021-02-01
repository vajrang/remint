from collections import Counter

import pandas as pd

# budgets = data from budgets.json (user defined budget values and groupings, aka budget_parents)
# parents = dict that allows lookups from category -> parent (as defined in mint)
# budget_parents = dict that allows lookups from category -> budget_parents


class BudgetGroups():
    def __init__(self, budgets: dict, parents: dict):
        self.budget_parents = BudgetGroups._process(budgets, parents)
        self.budgets = budgets
        self.parents = parents

    @staticmethod
    def _check_budgets(budgets: dict, parents: dict) -> bool:
        """Asserts that 
        1. all categories listed in the budgets config once and only once
        2. no category (in mint) is unaccounted for in the budgets
        """
        budgeted_categories = [c for _, v in budgets.values() for c in v]
        c = Counter(budgeted_categories)
        duplicated_categories = list(filter(lambda x: x if x[1] > 1 else None, [i for i in c.items()]))
        if duplicated_categories:
            print(f'Duplicated budgeted categories: {[c for c, _ in duplicated_categories]}')
            assert False

        all_categories = parents.keys()
        unbudgeted_categories = all_categories - budgeted_categories
        if unbudgeted_categories:
            print(f'Unbudgeted categories: {unbudgeted_categories}')
            assert False

    @staticmethod
    def _process(budgets: dict, parents: dict) -> dict:
        BudgetGroups._check_budgets(budgets, parents)

        retval = {}
        for budgetgroup, (value, categories) in budgets.items():
            for c in categories:
                retval[c] = budgetgroup

        return retval

    def get_budget_parents(self) -> dict:
        return self.budget_parents
