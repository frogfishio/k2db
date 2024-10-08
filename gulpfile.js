const { src, dest, series, parallel } = require('gulp');
const bump = require('gulp-bump');
const clean = require('gulp-clean');
const ts = require('gulp-typescript');
const jsonTransform = require('gulp-json-transform');

// Paths for tasks
const paths = {
  dist: 'dist/',
  tsFiles: 'src/**/*.ts',
  packageJson: 'package.json'
};

// TypeScript project
const tsProject = ts.createProject('tsconfig.json');

// Clean task: Cleans the `dist/` directory
function cleanDist() {
  return src(paths.dist, { read: false, allowEmpty: true }).pipe(clean());
}

// Transpile TypeScript files
function buildTs() {
  return tsProject
    .src()
    .pipe(tsProject())
    .pipe(dest(paths.dist));
}

// Copy non-TypeScript files (like `README`, `LICENSE`) to dist
function copyFiles() {
  return src(['README.md', 'LICENSE'], { allowEmpty: true }).pipe(dest(paths.dist));
}

// Bump version in `package.json` before copying
function bumpVersion() {
  return src(paths.packageJson)
    .pipe(bump({ type: 'patch' })) // Change to 'major', 'minor', or 'patch' as needed
    .pipe(dest('./')); // Save the updated `package.json`
}

// Copy and transform `package.json` for distribution
function copyPackageJson() {
  return src(paths.packageJson)
    .pipe(
      jsonTransform((pkg) => {
        // Remove unnecessary fields
        delete pkg.devDependencies;
        delete pkg.scripts;

        // Update paths to the `dist` directory
        pkg.main = 'data.js';
        pkg.types = 'data.d.ts';

        return pkg;
      }, 2)
    )
    .pipe(dest(paths.dist));
}

// Build tasks in series and parallel as needed
const build = series(cleanDist, buildTs, parallel(copyFiles, copyPackageJson));
const buildAndBump = series(bumpVersion, build);

// Export Gulp tasks
exports.clean = cleanDist;
exports.build = build;
exports.bump = bumpVersion;
exports.default = buildAndBump;