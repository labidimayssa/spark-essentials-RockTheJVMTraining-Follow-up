Pod Definition: We define a pod template that includes multiple containers (Java, Maven, and JFrog CLI).
Checkout Stage: We pull code from a Git repository (using a specified branch).


Pod Definition: We define a pod template that includes multiple containers (Java, Maven, and JFrog CLI).
1- Checkout Stage: We pull code from a Git repository (using a specified branch).
2- Get Image Tag Stage: We read the project’s pom.xml to extract the version (and derive the image tag).
3- Release Stage: Determine whether it’s a “pre-release” (snapshot) or a “release”.
4- Build/Test Stage: Perform a Maven build (and coverage analysis) using the provided credentials and settings.
5- Deploy to JFrog Stage: Use JFrog CLI in the jfrog-container to upload artifacts to either the snapshot or release Artifactory repository, depending on the release type.



------ details about Build/test stage :


This mvn command is what triggers the Maven build and test. Specifically:

mvn clean: removes any previous compiled artifacts.
scoverage:report: (assuming you have the scoverage plugin configured) will compile and run tests to generate the coverage report.



Why do we need JFrog here?
Likely because your Maven settings (${MAVEN_SETTINGS}) points to repositories hosted on JFrog Artifactory for downloading project dependencies during the build.
Even though you are not explicitly running any “jfrog” CLI command in this stage, Maven may need authentication to pull your project’s dependencies from a secured JFrog repository.


Do we upload dependencies to JFrog in this step?
No. In this build/test stage, you are only downloading dependencies to compile and test your code.
Uploading artifacts (your build outputs) to JFrog happens in the stage('Deploy to JFrog'), where you actually run the jf rt upload command.
So in summary, the JFrog credentials in the Build/test stage let Maven pull dependencies from a secured Artifactory repository, while the actual artifact upload to JFrog occurs in the Deploy to JFrog stage.

------Culture generale :

En règle générale, Artifactory est le nom du gestionnaire de dépôts proposé par JFrog. Il permet de stocker et de gérer toutes sortes d’artefacts (librairies Maven, images Docker, fichiers binaires, etc.). Quand on parle de « dépôts Artifactory », on fait référence aux dépôts hébergés dans cet outil JFrog.

Pourquoi Artifactory pour les dépendances Maven ?
Dans un projet Maven, vous avez besoin de librairies/dépendances externes (fichiers JAR, etc.). Par défaut, Maven va les télécharger depuis le Maven Central Repository ou d’autres repositories publics. Cependant, dans de nombreuses entreprises, on utilise Artifactory comme cache ou comme proxy pour gérer et sécuriser ces dépendances. Ainsi :

Vous configurez votre settings.xml Maven pour pointer vers votre serveur Artifactory (plutôt que Maven Central directement).
Quand Maven a besoin de télécharger une dépendance, il passe par Artifactory qui :
peut l’avoir déjà en cache, ou
peut aller la chercher sur internet (Maven Central, etc.) et la stocker localement.
C’est ce mécanisme qui fait qu’on parle de « dépôts Artifactory pour récupérer les dépendances Maven ».

Résumé
Artifactory est simplement le nom commercial du gestionnaire de dépôts créé par JFrog.
On configure Maven pour qu’il télécharge (et éventuellement mette en cache) les dépendances depuis un dépôt hébergé sur Artifactory.
L’étape de build et test d’un projet Maven s’appuie donc souvent sur Artifactory pour récupérer les artefacts dont il a besoin.


