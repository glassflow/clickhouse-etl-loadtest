import json
import random
from typing import List, Dict, Any, Union
import itertools
from dataclasses import dataclass
import math

@dataclass
class ParameterRange:
    min: Union[int, float]
    max: Union[int, float]
    step: Union[int, float]
    description: str

@dataclass
class ParameterValues:
    values: List[str]
    description: str

class LoadTestGenerator:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.parameters = self.config['parameters']
        self.test_config = self.config['test_combinations']
        self._validate_config()

    def _validate_config(self):
        """Validate the configuration file structure and values"""
        required_sections = ['parameters', 'test_combinations']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required section: {section}")

        # Validate max_combinations
        max_combinations = self.test_config.get('max_combinations')
        if max_combinations is None:
            raise ValueError("max_combinations is required in test_combinations")
        if not isinstance(max_combinations, int):
            raise ValueError("max_combinations must be an integer")
        if max_combinations < -1:
            raise ValueError("max_combinations must be -1 or greater")

        for param_name, param_config in self.parameters.items():
            if 'values' in param_config:
                if not isinstance(param_config['values'], list):
                    raise ValueError(f"Parameter {param_name} values must be a list")
            else:
                required_fields = ['min', 'max', 'step']
                for field in required_fields:
                    if field not in param_config:
                        raise ValueError(f"Parameter {param_name} missing required field: {field}")
                if param_config['min'] > param_config['max']:
                    raise ValueError(f"Parameter {param_name} min value greater than max value")

    def _generate_range_values(self, param_name: str) -> List[Any]:
        """Generate all possible values for a parameter based on its configuration"""
        param_config = self.parameters[param_name]
        
        if 'values' in param_config:
            return param_config['values']
        
        values = []
        current = param_config['min']
        while current <= param_config['max']:
            values.append(current)
            current += param_config['step']
            # Handle floating point precision
            if isinstance(current, float):
                current = round(current, 3)
        return values

    def _generate_grid_combinations(self) -> List[Dict[str, Any]]:
        """Generate all possible combinations of parameters (grid search)"""
        param_values = {
            name: self._generate_range_values(name)
            for name in self.parameters.keys()
        }
        
        # Generate all combinations
        keys = list(param_values.keys())
        values = list(param_values.values())
        combinations = list(itertools.product(*values))
        
        # Convert to list of dictionaries
        return [dict(zip(keys, combo)) for combo in combinations]

    def _generate_random_combinations(self) -> List[Dict[str, Any]]:
        """Generate random combinations of parameters"""
        max_combinations = self.test_config['max_combinations']
        if max_combinations == -1:
            # For random strategy, we need a finite number of combinations
            # Let's use the total possible combinations as a limit
            total_combinations = 1
            for param_config in self.parameters.values():
                if 'values' in param_config:
                    total_combinations *= len(param_config['values'])
                else:
                    steps = int((param_config['max'] - param_config['min']) / param_config['step']) + 1
                    total_combinations *= steps
            max_combinations = min(total_combinations, 1000)  # Cap at 1000 for random strategy
        
        combinations = []
        # Generate random combinations
        for _ in range(max_combinations):
            combination = {}
            for param_name, param_config in self.parameters.items():
                if 'values' in param_config:
                    combination[param_name] = random.choice(param_config['values'])
                else:
                    min_val = param_config['min']
                    max_val = param_config['max']
                    step = param_config['step']
                    # Generate random value within range, respecting step
                    steps = int((max_val - min_val) / step)
                    random_step = random.randint(0, steps)
                    value = min_val + (random_step * step)
                    if isinstance(value, float):
                        value = round(value, 3)
                    combination[param_name] = value
            combinations.append(combination)
        
        return combinations

    def generate_combinations(self) -> List[Dict[str, Any]]:
        """Generate test combinations based on the configured strategy"""
        if self.test_config['strategy'] == 'grid':
            combinations = self._generate_grid_combinations()
        else:  # random
            combinations = self._generate_random_combinations()

        # Limit the number of combinations if needed
        max_combinations = self.test_config['max_combinations']
        if max_combinations != -1 and len(combinations) > max_combinations:
            if self.test_config['strategy'] == 'grid':
                # For grid search, we'll sample evenly across the space
                step = len(combinations) / max_combinations
                indices = [int(i * step) for i in range(max_combinations)]
                combinations = [combinations[i] for i in indices]
            else:
                # For random, we'll just take the first max_combinations
                combinations = combinations[:max_combinations]

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
    generator = LoadTestGenerator('config/load_test_params.json')
    combinations = generator.generate_combinations()
    for combo in combinations:
        print(json.dumps(combo, indent=2))
    # generator.print_combinations(combinations)

if __name__ == "__main__":
    main() 