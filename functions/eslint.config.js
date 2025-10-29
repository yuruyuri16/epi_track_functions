import prettier from 'eslint-plugin-prettier'

export default [
  {
    files: ['**/*.{js,jsx,ts,tsx}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        // Node globals
        module: 'readonly',
        __dirname: 'readonly',
        __filename: 'readonly',
        require: 'readonly',
        process: 'readonly',
      },
    },
    plugins: { prettier },
    rules: {
      'no-restricted-globals': ['error', 'name', 'length'],
      'prefer-arrow-callback': 'error',
      'prettier/prettier': 'error',
    },
  },
  {
    files: ['**/*.spec.*'],
    languageOptions: {
      globals: { mocha: true },
    },
  },
]
