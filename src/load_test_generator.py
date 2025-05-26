import json
from typing import List, Dict, Any
import itertools
import sys
from src.models import LoadTestConfig, ParameterValues

class LoadTestGenerator:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = LoadTestConfig.model_validate_json(f.read())
        self.parameters = self.config.parameters

    def _generate_range_values(self, param_name: str) -> List[Any]:
        """Generate all possible values for a parameter based on its configuration"""
        param_config = getattr(self.parameters, param_name)
        
        if isinstance(param_config, ParameterValues):
            return param_config.values
        
        values = []
        current = param_config.min
        while current <= param_config.max:
            values.append(current)
            current += param_config.step
            # Handle floating point precision
            if isinstance(current, float):
                current = round(current, 3)
        return values

    def _generate_grid_combinations(self) -> List[Dict[str, Any]]:
        """Generate all possible combinations of parameters (grid search)"""
        param_values = {
            name: self._generate_range_values(name)
            for name in self.parameters.__class__.model_fields.keys()
        }
        
        # Generate all combinations
        keys = list(param_values.keys())
        values = list(param_values.values())
        combinations = list(itertools.product(*values))
        
        # Convert to list of dictionaries
        return [dict(zip(keys, combo)) for combo in combinations]

    def generate_combinations(self) -> List[Dict[str, Any]]:
        """Generate test combinations using grid search"""
        combinations = self._generate_grid_combinations()

        # Limit the number of combinations if needed
        max_combinations = self.config.max_combinations
        if max_combinations != -1 and len(combinations) > max_combinations:
            # Sample evenly across the space
            step = len(combinations) / max_combinations
            indices = [int(i * step) for i in range(max_combinations)]
            combinations = [combinations[i] for i in indices]

        return combinations

    def print_combinations(self, combinations: List[Dict[str, Any]]):
        """Print the generated combinations in a readable format"""
        print(f"\nGenerated {len(combinations)} test combinations:")
        for i, combo in enumerate(combinations, 1):
            print(f"\nCombination {i}:")
            for param, value in combo.items():
                print(f"  {param}: {value}")

def main():
    # Example usage
    config_path = sys.argv[1]
    generator = LoadTestGenerator(config_path)
    combinations = generator.generate_combinations()
    generator.print_combinations(combinations)

if __name__ == "__main__":
    main() 