# DistriLab — CS324 Assignment 1

Java 21 / Maven distributed computing using Java RMI.

## Build
`mvn clean verify`

## Architecture
Shared serializable data and RMI interfaces live in `edu.usp.cs324.api`.
The network implementation loads a `JobEngine` through Java ServiceLoader,
allowing both feature branches to compile independently from this shared baseline.

## Running and verification
Startup commands, examples, election assumptions and integration verification
will be completed alongside the feature implementations.

## Development
The shared contracts precede `feature/network-election` and `feature/jobs-client`.
Commits record actual implementation and verification milestones. Code is
AI-assisted; Git history does not represent separate human group contributions.
