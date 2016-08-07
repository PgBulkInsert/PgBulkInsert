:: Copyright (c) Philipp Wagner. All rights reserved.
:: Licensed under the MIT license. See LICENSE file in the project root for full license information.

@echo off

echo ---------------------------------------------------
echo - Bundling Artifacts for Manual Repository Upload -
echo ---------------------------------------------------

:: Define the Executables, so we don't have to rely on pathes:
set JAR_EXECUTABLE="C:\Program Files\Java\jdk1.8.0_71\bin\jar.exe"
set GPG_EXECUTABLE="C:\Program Files (x86)\GNU\GnuPG\pub\gpg.exe"

:: Logs to be used:
set STDOUT=stdout.log
set STDERR=stderr.log

:: Version to build the bundle for:
set VERSION=1.1

:: Set the Target Bundle file:
set TARGET_BUNDLE=bundle\pgbulkinsert-bundle-%VERSION%.jar

:: Define the Artifacts to be signed. Simply use an absolute path here:
set ARTIFACTS_DIR=artifacts
set BASE_DIR=%ARTIFACTS_DIR%\%VERSION%

:: Set the Filename for artifacts to use:
set FILENAME=pgbulkinsert-%VERSION%

set JAR_FILE=%FILENAME%.jar
set JAR_SOURCES_FILE=%FILENAME%-sources.jar
set JAR_JAVADOC_FILE=%FILENAME%-javadoc.jar

set POM_FILE=pom.xml

set JAR_FILE_ASC=%JAR_FILE%.asc
set POM_FILE_ASC=%POM_FILE%.asc
set JAR_SOURCES_FILE_ASC=%JAR_SOURCES_FILE%.asc
set JAR_JAVADOC_FILE_ASC=%JAR_JAVADOC_FILE%.asc

:: Ask for GPG Signing Passphrase:

set /p PASSPHRASE="Signing Passphrase: "

1>%STDOUT% 2>%STDERR% (
    
    :: Create Fake:
    %JAR_EXECUTABLE% -cf %BASE_DIR%\%JAR_SOURCES_FILE% -C %ARTIFACTS_DIR% README.txt
    %JAR_EXECUTABLE% -cf %BASE_DIR%\%JAR_JAVADOC_FILE% -C %ARTIFACTS_DIR% README.txt
    
    :: Sign JAR and POM Files:
    echo %PASSPHRASE%|%GPG_EXECUTABLE% --batch --yes --passphrase-fd 0  -b -a -s "%BASE_DIR%\%JAR_FILE%"  
    echo %PASSPHRASE%|%GPG_EXECUTABLE% --batch --yes --passphrase-fd 0  -b -a -s "%BASE_DIR%\%POM_FILE%"
    echo %PASSPHRASE%|%GPG_EXECUTABLE% --batch --yes --passphrase-fd 0  -b -a -s "%BASE_DIR%\%JAR_SOURCES_FILE%"
    echo %PASSPHRASE%|%GPG_EXECUTABLE% --batch --yes --passphrase-fd 0  -b -a -s "%BASE_DIR%\%JAR_JAVADOC_FILE%"

    :: Create the Bundle File for manual upload:
    %JAR_EXECUTABLE% -cf "%TARGET_BUNDLE%" -C "%BASE_DIR%" "%JAR_FILE%" -C "%BASE_DIR%" "%JAR_FILE_ASC%" -C "%BASE_DIR%" "%POM_FILE%" -C "%BASE_DIR%" "%POM_FILE_ASC%" -C "%BASE_DIR%" "%JAR_SOURCES_FILE%" -C "%BASE_DIR%" "%JAR_JAVADOC_FILE%" -C "%BASE_DIR%" "%JAR_SOURCES_FILE_ASC%" -C "%BASE_DIR%" "%JAR_JAVADOC_FILE_ASC%"
    
)

pause