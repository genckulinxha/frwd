#!/usr/bin/env python3
"""
LLM Token Counter

A utility script to count tokens for various Language Learning Models.
Supports OpenAI models (GPT-3.5, GPT-4), Claude, and other popular models.

Usage:
    python token_counter.py "Your text here"
    python token_counter.py --file input.txt
    python token_counter.py --model gpt-4 "Your text here"
    python token_counter.py --interactive
"""

import argparse
import sys
from typing import Optional, Dict, Any
import os

try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False

try:
    from transformers import AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


class TokenCounter:
    """Token counter for various LLM models."""
    
    SUPPORTED_MODELS = {
        # OpenAI models (requires tiktoken)
        'gpt-4': 'cl100k_base',
        'gpt-4-turbo': 'cl100k_base',
        'gpt-4o': 'cl100k_base',
        'gpt-3.5-turbo': 'cl100k_base',
        'text-davinci-003': 'p50k_base',
        'text-davinci-002': 'p50k_base',
        'text-ada-001': 'r50k_base',
        'text-babbage-001': 'r50k_base',
        'text-curie-001': 'r50k_base',
        
        # Claude models (approximate using cl100k_base)
        'claude-3-opus': 'cl100k_base',
        'claude-3-sonnet': 'cl100k_base',
        'claude-3-haiku': 'cl100k_base',
        'claude-2': 'cl100k_base',
        'claude-instant': 'cl100k_base',
    }
    
    def __init__(self):
        """Initialize the token counter."""
        self.tokenizers = {}
    
    def get_tokenizer(self, model: str):
        """Get or create a tokenizer for the specified model."""
        if model in self.tokenizers:
            return self.tokenizers[model]
        
        if model not in self.SUPPORTED_MODELS:
            raise ValueError(f"Unsupported model: {model}. Supported models: {list(self.SUPPORTED_MODELS.keys())}")
        
        encoding_name = self.SUPPORTED_MODELS[model]
        
        if not TIKTOKEN_AVAILABLE:
            raise ImportError("tiktoken is required for token counting. Install with: pip install tiktoken")
        
        try:
            tokenizer = tiktoken.get_encoding(encoding_name)
            self.tokenizers[model] = tokenizer
            return tokenizer
        except Exception as e:
            raise RuntimeError(f"Failed to load tokenizer for {model}: {e}")
    
    def count_tokens(self, text: str, model: str = 'gpt-4') -> Dict[str, Any]:
        """Count tokens for the given text and model."""
        tokenizer = self.get_tokenizer(model)
        tokens = tokenizer.encode(text)
        
        return {
            'model': model,
            'token_count': len(tokens),
            'character_count': len(text),
            'word_count': len(text.split()),
            'encoding': self.SUPPORTED_MODELS[model]
        }
    
    def count_tokens_approximate(self, text: str) -> Dict[str, Any]:
        """Provide approximate token count using simple heuristics."""
        # Rough approximation: 1 token ≈ 4 characters for English text
        char_count = len(text)
        word_count = len(text.split())
        approx_tokens = max(char_count // 4, word_count)  # Use the higher estimate
        
        return {
            'model': 'approximate',
            'token_count': approx_tokens,
            'character_count': char_count,
            'word_count': word_count,
            'encoding': 'heuristic (4 chars ≈ 1 token)'
        }


def read_file(filepath: str) -> str:
    """Read text from a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)
    except UnicodeDecodeError:
        print(f"Error: Unable to decode file '{filepath}'. Please ensure it's a text file.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{filepath}': {e}")
        sys.exit(1)


def print_results(results: Dict[str, Any], show_details: bool = True):
    """Print token counting results in a formatted way."""
    print(f"\n📊 Token Count Results")
    print("=" * 40)
    print(f"Model: {results['model']}")
    print(f"Tokens: {results['token_count']:,}")
    
    if show_details:
        print(f"Characters: {results['character_count']:,}")
        print(f"Words: {results['word_count']:,}")
        print(f"Encoding: {results['encoding']}")
        
        # Cost estimates for popular models (approximate prices as of 2024)
        if results['model'].startswith('gpt-4'):
            input_cost = (results['token_count'] / 1000) * 0.03  # $0.03 per 1K tokens
            print(f"Estimated cost (input): ${input_cost:.4f}")
        elif results['model'].startswith('gpt-3.5'):
            input_cost = (results['token_count'] / 1000) * 0.001  # $0.001 per 1K tokens
            print(f"Estimated cost (input): ${input_cost:.4f}")


def interactive_mode():
    """Run in interactive mode for multiple text inputs."""
    counter = TokenCounter()
    print("🤖 LLM Token Counter - Interactive Mode")
    print("Type 'quit' or 'exit' to stop")
    print("Type 'models' to see available models")
    print("Type 'model <name>' to change the current model")
    print("-" * 50)
    
    current_model = 'gpt-4'
    print(f"Current model: {current_model}")
    
    while True:
        try:
            user_input = input("\nEnter text (or command): ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("Goodbye! 👋")
                break
            elif user_input.lower() == 'models':
                print("Available models:")
                for model in counter.SUPPORTED_MODELS.keys():
                    print(f"  - {model}")
                continue
            elif user_input.lower().startswith('model '):
                new_model = user_input[6:].strip()
                if new_model in counter.SUPPORTED_MODELS:
                    current_model = new_model
                    print(f"Model changed to: {current_model}")
                else:
                    print(f"Unknown model: {new_model}")
                continue
            elif not user_input:
                continue
            
            if TIKTOKEN_AVAILABLE:
                results = counter.count_tokens(user_input, current_model)
            else:
                results = counter.count_tokens_approximate(user_input)
                print("⚠️  Using approximate counting (install tiktoken for accurate results)")
            
            print_results(results, show_details=False)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye! 👋")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main function to handle command line arguments and execute token counting."""
    parser = argparse.ArgumentParser(
        description="Count tokens for various LLM models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python token_counter.py "Hello, world!"
  python token_counter.py --file document.txt
  python token_counter.py --model gpt-3.5-turbo "Your text here"
  python token_counter.py --interactive
  python token_counter.py --models
        """
    )
    
    parser.add_argument('text', nargs='?', help='Text to count tokens for')
    parser.add_argument('--file', '-f', help='Read text from file')
    parser.add_argument('--model', '-m', default='gpt-4', 
                       help='Model to use for token counting (default: gpt-4)')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Run in interactive mode')
    parser.add_argument('--models', action='store_true',
                       help='List all supported models')
    parser.add_argument('--approximate', '-a', action='store_true',
                       help='Use approximate counting (no dependencies required)')
    
    args = parser.parse_args()
    
    counter = TokenCounter()
    
    # Handle special commands
    if args.models:
        print("Supported models:")
        for model in counter.SUPPORTED_MODELS.keys():
            print(f"  - {model}")
        return
    
    if args.interactive:
        interactive_mode()
        return
    
    # Determine input text
    text = None
    if args.file:
        text = read_file(args.file)
    elif args.text:
        text = args.text
    else:
        parser.print_help()
        return
    
    if not text:
        print("Error: No text provided")
        return
    
    # Count tokens
    try:
        if args.approximate or not TIKTOKEN_AVAILABLE:
            if not TIKTOKEN_AVAILABLE:
                print("⚠️  tiktoken not available. Using approximate counting.")
                print("   Install with: pip install tiktoken")
            results = counter.count_tokens_approximate(text)
        else:
            results = counter.count_tokens(text, args.model)
        
        print_results(results)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 