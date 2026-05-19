import js from '@eslint/js'
import tseslint from 'typescript-eslint'
import pluginN from 'eslint-plugin-n'
import pluginMocha from 'eslint-plugin-mocha'
import pluginJsdoc from 'eslint-plugin-jsdoc'
import prettierConfig from 'eslint-config-prettier'
import globals from 'globals'

/**
 * Defines the AST nodes we expect to have documentation for.  It includes
 * most publicly defined stuff with the following exceptions:
 * - No need for descriptions on Options interfaces.
 * - No need to document setters.
 * - No need to document protected or private.
 * - No need to document inline types in function parameters.
 */
const needsDocsContexts = [
  'TSInterfaceDeclaration[id.name!=/.*Options/]',
  'TSTypeAliasDeclaration',
  'TSEnumDeclaration',
  'TSEnumMember',
  'TSMethodSignature[accessibility!=/(private|protected)/]',
  'ClassBody > TSPropertySignature[accessibility!=/(private|protected)/]',
  'TSInterfaceBody > TSPropertySignature[accessibility!=/(private|protected)/]',
  'FunctionDeclaration',
  'ClassDeclaration',
  'MethodDefinition[accessibility!=/(private|protected)/][kind!=/(set|constructor)/]',
  'ClassBody > ClassProperty[accessibility!=/(private|protected)/]',
]

export default [
  {
    ignores: [
      'dist/**',
      'node_modules/**',
      'coverage/**',
      '**/*.d.ts',
      'deps/**',
      'scripts/**',
      'tools/**',
    ],
  },

  js.configs.recommended,

  ...tseslint.configs.recommended,

  {
    files: ['lib/**/*.ts', 'test/**/*.{js,ts}'],

    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
      },
      globals: {
        ...globals.node,
        ...globals.es2021,
        ...globals.mocha,
      },
    },

    plugins: {
      '@typescript-eslint': tseslint.plugin,
      n: pluginN,
      mocha: pluginMocha,
      jsdoc: pluginJsdoc,
    },

    settings: {
      jsdoc: {
        ignorePrivate: true,
        ignoreInternal: true,
      },
    },

    rules: {
      '@typescript-eslint/explicit-module-boundary-types': [
        'error',
        {
          allowArgumentsExplicitlyTypedAsAny: true,
        },
      ],
      '@typescript-eslint/no-explicit-any': 'off',

      '@typescript-eslint/no-empty-object-type': [
        'error',
        {
          allowInterfaces: 'always',
        },
      ],

      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],

      'n/no-unsupported-features/es-syntax': [
        'error',
        {
          ignores: ['modules'],
        },
      ],
      'n/no-missing-import': [
        'error',
        {
          tryExtensions: ['.js', '.ts'],
        },
      ],
      'n/no-missing-require': [
        'error',
        {
          tryExtensions: ['.js', '.ts'],
        },
      ],

      'jsdoc/check-tag-names': [
        'warn',
        {
          definedTags: ['category', 'internal', 'experimental'],
        },
      ],
      'jsdoc/require-jsdoc': [
        'warn',
        {
          contexts: needsDocsContexts,
        },
      ],
      'jsdoc/require-description': [
        'warn',
        {
          contexts: needsDocsContexts,
        },
      ],
      'jsdoc/require-description-complete-sentence': 'warn',
      'jsdoc/require-returns': 'off',
      'jsdoc/require-param-type': 'off',
      'jsdoc/tag-lines': [
        'warn',
        'any',
        {
          startLines: 1,
        },
      ],
      'jsdoc/no-undefined-types': [
        'warn',
        {
          definedTypes: [
            'durabilityLevel',
            'effectiveRoles',
            'GetOptions',
            'IBucketSettings',
            'MutationState',
            'StorageBackend',
          ],
        },
      ],

      'prefer-rest-params': 'off',
    },
  },

  {
    files: ['test/**/*.{js,ts}'],
    rules: {
      '@typescript-eslint/no-var-requires': 'off',
      '@typescript-eslint/no-require-imports': 'off',
      'jsdoc/require-jsdoc': 'off',
    },
  },

  prettierConfig,
]
