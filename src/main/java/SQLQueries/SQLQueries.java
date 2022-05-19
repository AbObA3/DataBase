package SQLQueries;

import Database.DataBaseImpl;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class SQLQueries {
    private static final Logger log;

    static {
        log = Logger.getLogger(DataBaseImpl.class.getName());
    }

    public List<String> sqlQueriesCreate;
    NormalDistribution normalDistribution;
    HashMap<String, UniformIntegerDistribution> distributions;
    private List<String> engMenNames;
    private List<String> engSurnames;
    private List<String> engWomenNames;
    private List<String> rusMenNames;
    private List<String> rusMenSurnames;
    private List<String> rusWomenSurnames;
    private List<String> rusWomenNames;
    private List<String> Albums;
    private List<String> Alias;
    private List<String> Genres;
    private List<String> GroupNames;
    private List<String> Labels;
    private List<String> MusicalCompositions;
    private List<List<List<String>>> names;
    private List<List<List<String>>> surnames;
    private List<String> volumeType;
    private List<String> recordType;
    private List<String> conceptType;

    public SQLQueries() {
        distributions = new HashMap<>();
        sqlQueriesCreate = new ArrayList<>();
        sqlQueriesCreate.add("DROP DATABASE if exists musicdatabase ;");
        sqlQueriesCreate.add("CREATE SCHEMA `musicdatabase` ;");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`composer` (" +
                "                                            `id_composer` INT NOT NULL AUTO_INCREMENT," +
                "                                            `Second name` VARCHAR(30) NOT NULL," +
                "                                            `Name` VARCHAR(30) NOT NULL," +
                "                                            PRIMARY KEY (`id_composer`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`text author` (" +
                "                                               `id_text_author` INT NOT NULL AUTO_INCREMENT," +
                "                                               `Second name` VARCHAR(30) NOT NULL," +
                "                                               `Name` VARCHAR(30) NOT NULL," +
                "                                               PRIMARY KEY (`id_text_author`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`musical producer` (" +
                "                                                    `id_musical_producer` INT NOT NULL AUTO_INCREMENT," +
                "                                                    `Alias` VARCHAR(30) NULL," +
                "                                                    `Second name` VARCHAR(30) NOT NULL," +
                "                                                    `Name` VARCHAR(30) NULL," +
                "                                                    PRIMARY KEY (`id_musical_producer`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`label` (" +
                "                                         `id_label` INT NOT NULL AUTO_INCREMENT," +
                "                                         `Name` VARCHAR(60) NOT NULL," +
                "                                         PRIMARY KEY (`id_label`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`genre` (" +
                "                                         `id_genre` INT NOT NULL AUTO_INCREMENT," +
                "                                         `Kind of genre` VARCHAR(30) NOT NULL," +
                "                                         PRIMARY KEY (`id_genre`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`group name` (" +
                "                                              `id_group_name` INT NOT NULL AUTO_INCREMENT," +
                "                                              `Name` VARCHAR(60) NOT NULL," +
                "                                              PRIMARY KEY (`id_group_name`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`performer` (" +
                "                                             `id_performer` INT NOT NULL AUTO_INCREMENT," +
                "                                             `Alias` VARCHAR(60) NOT NULL," +
                "                                             PRIMARY KEY (`id_performer`));");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`album` (" +
                "                                         `id_album` INT NOT NULL AUTO_INCREMENT," +
                "                                         `Name` VARCHAR(100) NOT NULL," +
                "                                         `Date of issue` VARCHAR(30) NOT NULL," +
                "                                         `Volume type` VARCHAR(30) NOT NULL," +
                "                                         `Record type` VARCHAR(30) NOT NULL," +
                "                                         `Concept type` VARCHAR(30) NOT NULL," +
                "                                         `id_label` INT NOT NULL," +
                "                                         PRIMARY KEY (`id_album`)," +
                "                                         CONSTRAINT `id_label`" +
                "                                             FOREIGN KEY (`id_label`)" +
                "                                                 REFERENCES `musicdatabase`.`label` (`id_label`)" +
                "                                                 ON DELETE RESTRICT" +
                "                                                 ON UPDATE RESTRICT);");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`composition` (" +
                "                                               `id_composition` INT NOT NULL AUTO_INCREMENT," +
                "                                               `id_group_name` INT NOT NULL," +
                "                                               PRIMARY KEY (`id_composition`)," +
                "                                               CONSTRAINT `id_group_name`" +
                "                                                   FOREIGN KEY (`id_group_name`)" +
                "                                                       REFERENCES `musicdatabase`.`group name` (`id_group_name`)" +
                "                                                       ON DELETE RESTRICT" +
                "                                                       ON UPDATE RESTRICT);");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`musical composition` (" +
                "                                                       `id_musical_composition` INT NOT NULL AUTO_INCREMENT," +
                "                                                       `Name` VARCHAR(100) NOT NULL," +
                "                                                       `Date of issue` VARCHAR(30) NOT NULL," +
                "                                                       `id_composer` INT NOT NULL," +
                "                                                       `id_text_author` INT NOT NULL," +
                "                                                       `id_composition` INT NOT NULL," +
                "                                                       `id_album` INT NOT NULL," +
                "                                                       `id_musical_producer` INT NOT NULL," +
                "                                                       PRIMARY KEY (`id_musical_composition`)," +
                "                                                       CONSTRAINT `id_composer`" +
                "                                                           FOREIGN KEY (`id_composer`)" +
                "                                                               REFERENCES `musicdatabase`.`composer` (`id_composer`)" +
                "                                                               ON DELETE RESTRICT" +
                "                                                               ON UPDATE RESTRICT," +
                "                                                       CONSTRAINT `id_text_author`" +
                "                                                           FOREIGN KEY (`id_text_author`)" +
                "                                                               REFERENCES `musicdatabase`.`text author` (`id_text_author`)" +
                "                                                               ON DELETE RESTRICT" +
                "                                                               ON UPDATE RESTRICT," +
                "                                                       CONSTRAINT `id_composition`" +
                "                                                           FOREIGN KEY (`id_composition`)" +
                "                                                               REFERENCES `musicdatabase`.`composition` (`id_composition`)" +
                "                                                               ON DELETE RESTRICT" +
                "                                                               ON UPDATE RESTRICT," +
                "                                                       CONSTRAINT `id_album`" +
                "                                                           FOREIGN KEY (`id_album`)" +
                "                                                               REFERENCES `musicdatabase`.`album` (`id_album`)" +
                "                                                               ON DELETE RESTRICT" +
                "                                                               ON UPDATE RESTRICT," +
                "                                                       CONSTRAINT `id_musical_producer`" +
                "                                                           FOREIGN KEY (`id_musical_producer`)" +
                "                                                               REFERENCES `musicdatabase`.`musical producer` (`id_musical_producer`)" +
                "                                                               ON DELETE RESTRICT" +
                "                                                               ON UPDATE RESTRICT);");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`gm` (" +
                "                                      `id_gm` INT NOT NULL AUTO_INCREMENT," +
                "                                      `id_musical_composition` INT NOT NULL," +
                "                                      `id_genre` INT NOT NULL," +
                "                                      PRIMARY KEY (`id_gm`)," +
                "                                      CONSTRAINT `id_musical_composition`" +
                "                                          FOREIGN KEY (`id_musical_composition`)" +
                "                                              REFERENCES `musicdatabase`.`musical composition` (`id_musical_composition`)" +
                "                                              ON DELETE RESTRICT" +
                "                                              ON UPDATE RESTRICT," +
                "                                      CONSTRAINT `id_genre`" +
                "                                          FOREIGN KEY (`id_genre`)" +
                "                                              REFERENCES `musicdatabase`.`genre` (`id_genre`)" +
                "                                              ON DELETE RESTRICT" +
                "                                              ON UPDATE RESTRICT);");
        sqlQueriesCreate.add("CREATE TABLE `musicdatabase`.`cp` (" +
                "                                      `id_cp` INT NOT NULL AUTO_INCREMENT," +
                "                                      `id_composition` INT NOT NULL," +
                "                                      `id_performer` INT NOT NULL," +
                "                                      PRIMARY KEY (`id_cp`)," +
                "                                      CONSTRAINT `id_compositionCP`" +
                "                                          FOREIGN KEY (`id_composition`)" +
                "                                              REFERENCES `musicdatabase`.`composition` (`id_composition`)" +
                "                                              ON DELETE RESTRICT" +
                "                                              ON UPDATE RESTRICT," +
                "                                      CONSTRAINT `id_performer`" +
                "                                          FOREIGN KEY (`id_performer`)" +
                "                                              REFERENCES `musicdatabase`.`performer` (`id_performer`)" +
                "                                              ON DELETE RESTRICT" +
                "                                              ON UPDATE RESTRICT);");

        initLists();
    }

    public void initLists() {
        try {
            engMenNames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/engMenNames.txt")).split(", ")));
            engSurnames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/engSurnames.txt")).split(", ")));
            engWomenNames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/engWomenNames.txt")).split(", ")));
            rusMenNames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/rusMenNames.txt")).split(",")));
            rusMenSurnames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/rusMenSurnames.txt")).split(",")));
            rusWomenNames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/rusWomenNames.txt")).split(",")));
            rusWomenSurnames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/rusWomenSurnames.txt")).split(",")));
            Albums = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/Albums.txt")).split("\r\n")));
            Alias = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/Alias.txt")).split("\r\n")));
            Genres = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/Genres.txt")).split("\r\n")));
            GroupNames = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/GroupNames.txt")).split("\r\n")));
            Labels = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/Labels.txt")).split("\r\n")));
            MusicalCompositions = new ArrayList<>
                    (Arrays.asList(Files.readString(Paths
                            .get("src/main/resources/MusicalCompositions.txt")).split("\r\n")));
            volumeType = new ArrayList<>(List.of("Single", "LP", "EP", "Double", "Triple", "Box-set"));
            recordType = new ArrayList<>(List.of("Studio", "Concert", "Digest"));
            conceptType = new ArrayList<>
                    (List.of("Demo", "Debut",
                            "Conceptual", "Promo",
                            "Soundtrack", "Collaborative",
                            "Solo", "Split", "Tribute", "Mixtape"));

        } catch (IOException ex) {
            log.severe(ex.getMessage());
        }

        names = new ArrayList<>
                (Arrays.asList(Arrays.asList(engMenNames, rusMenNames), Arrays.asList(engWomenNames, rusWomenNames)));

        surnames = new ArrayList<>
                (Arrays.asList(Arrays.asList(engSurnames, rusMenSurnames), Arrays.asList(engSurnames, rusWomenSurnames)));
        distributions.put("menWomen", new UniformIntegerDistribution(0, 1));
        distributions.put("rusEng", new UniformIntegerDistribution(0, 1));
        distributions.put("label", new UniformIntegerDistribution(1, 20));
        distributions.put("genre", new UniformIntegerDistribution(1, 10));
        distributions.put("groupName", new UniformIntegerDistribution(1, 100));
        distributions.put("composition", new UniformIntegerDistribution(1, 1000));
        distributions.put("composerTextAuthorProducer", new UniformIntegerDistribution(1, 50));
        distributions.put("album", new UniformIntegerDistribution(1, 1000));
        distributions.put("musicalComposition", new UniformIntegerDistribution(1, 100000));
        normalDistribution = new NormalDistribution(250, 144);
    }


    public String fillingComposer() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`composer`(`id_composer`,`Second name`,`Name`)VALUES");
        for (int i = 1; i <= 50; i++) {
            int menWomen = distributions.get("menWomen").sample();
            int rusEng = distributions.get("rusEng").sample();
            List<String> surnames = this.surnames.get(menWomen).get(rusEng);
            String surname = surnames.get(new UniformIntegerDistribution(0, surnames.size() - 1).sample());
            List<String> names = this.names.get(menWomen).get(rusEng);
            String name = names.get(new UniformIntegerDistribution(0, names.size() - 1).sample());
            query.append("(").append(i).append(",'").append(surname).append("','").append(name).append("')");
            if (i != 50) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingTextAuthor() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`text author`(`id_text_author`,`Second name`,`Name`)VALUES");
        for (int i = 1; i <= 50; i++) {
            int menWomen = distributions.get("menWomen").sample();
            int rusEng = distributions.get("rusEng").sample();
            List<String> surnames = this.surnames.get(menWomen).get(rusEng);
            String surname = surnames.get(new UniformIntegerDistribution(0, surnames.size() - 1).sample());
            List<String> names = this.names.get(menWomen).get(rusEng);
            String name = names.get(new UniformIntegerDistribution(0, names.size() - 1).sample());
            query.append("(").append(i).append(",'").append(surname).append("','").append(name).append("')");
            if (i != 50) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingMusicalProducer() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`musical producer`(`id_musical_producer`,`Alias`,`Second name`,`Name`)VALUES");
        for (int i = 1; i <= 50; i++) {
            int menWomen = distributions.get("menWomen").sample();
            int rusEng = distributions.get("rusEng").sample();
            List<String> surnames = this.surnames.get(menWomen).get(rusEng);
            String surname = surnames.get(new UniformIntegerDistribution(0, surnames.size() - 1).sample());
            List<String> names = this.names.get(menWomen).get(rusEng);
            String alias = Alias.get(new UniformIntegerDistribution(0, Alias.size() - 1).sample());
            String name = names.get(new UniformIntegerDistribution(0, names.size() - 1).sample());
            query.append("(").append(i).append(",'").append(alias).append("','")
                    .append(surname).append("','").append(name).append("')");
            if (i != 50) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();

    }

    public String fillingLabel() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`label`(`id_label`,`Name`)VALUES");
        for (int i = 1; i <= 20; i++) {
            query.append("(").append(i).append(",'")
                    .append(Labels.get(new UniformIntegerDistribution(0, Labels.size() - 1).sample()))
                    .append("')");
            if (i != 20) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingGenre() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`genre`(`id_genre`,`Kind of genre`)VALUES");
        for (int i = 1; i <= 10; i++) {
            query.append("(").append(i).append(",'")
                    .append(Genres.get(i))
                    .append("')");
            if (i != 10) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingGroupName() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`group name`(`id_group_name`,`Name`)VALUES");
        for (int i = 1; i <= 100; i++) {
            query.append("(").append(i).append(",'")
                    .append(GroupNames.get(new UniformIntegerDistribution(0, GroupNames.size() - 1).sample()))
                    .append("')");
            if (i != 100) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingPerformer() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`performer`(`id_performer`,`Alias`)VALUES");
        for (int i = 1; i <= 500; i++) {
            query.append("(").append(i).append(",'")
                    .append(Alias.get(new UniformIntegerDistribution(0, Alias.size() - 1).sample()))
                    .append("')");
            if (i != 500) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingComposition() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`composition`(`id_composition`,`id_group_name`)VALUES");
        for (int i = 1; i <= 1000; i++) {
            query.append("(").append(i).append(",")
                    .append(distributions.get("groupName").sample())
                    .append(")");
            if (i != 1000) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingCP() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`cp`(`id_cp`,`id_composition`,`id_performer`)VALUES");
        int[] rep = new int[501];
        Arrays.fill(rep, 0);
        int v;
        for (int i = 1; i <= 2500; i++) {
            if (i > 500) {
                do {
                    do {
                        v = (int) Math.round(normalDistribution.sample());
                    } while (v < 1 || v > 500);
                } while (rep[v] >= 10);
            } else {
                v = i;
            }
            query.append("(").append(i).append(",")
                    .append(distributions.get("composition").sample()).append(",")
                    .append(v)
                    .append(")");
            rep[v]++;
            if (i != 2500) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingAlbum() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`album`" +
                "(`id_album`, `Name`, `Date of issue`,`Volume type`,`Record type`,`Concept type`,`id_label`)VALUES");
        for (int i = 1; i <= 1000; i++) {
            query.append("(").append(i).append(",'")
                    .append(Albums.get(new UniformIntegerDistribution(0, Albums.size() - 1).sample()))
                    .append("','").append(new UniformIntegerDistribution(1900, 2021).sample())
                    .append("-").append(new UniformIntegerDistribution(1, 12).sample())
                    .append("-").append(new UniformIntegerDistribution(1, 28).sample())
                    .append("','")
                    .append(volumeType.get(new UniformIntegerDistribution(0, volumeType.size() - 1).sample()))
                    .append("','").append(recordType.get(new UniformIntegerDistribution(0, recordType.size() - 1).sample()))
                    .append("','").append(conceptType.get(new UniformIntegerDistribution(0, conceptType.size() - 1).sample()))
                    .append("',").append(distributions.get("label").sample()).append(")");
            if (i != 1000) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingMusicalComposition() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`musical composition`" +
                "(`id_musical_composition`, `Name`, `Date of issue`,`id_composer`,`id_text_author`,`id_composition`,`id_album`,`id_musical_producer`)VALUES");
        for (int i = 1; i <= 100000; i++) {
            query.append("(").append(i).append(",'")
                    .append(MusicalCompositions
                            .get(new UniformIntegerDistribution(0, MusicalCompositions.size() - 1).sample()))
                    .append("','").append(new UniformIntegerDistribution(1900, 2021).sample())
                    .append("-").append(new UniformIntegerDistribution(1, 12).sample())
                    .append("-").append(new UniformIntegerDistribution(1, 28).sample())
                    .append("',")
                    .append(distributions.get("composerTextAuthorProducer").sample())
                    .append(",").append(distributions.get("composerTextAuthorProducer").sample())
                    .append(",").append(distributions.get("composition").sample())
                    .append(",").append(distributions.get("album").sample())
                    .append(",").append(distributions.get("composerTextAuthorProducer").sample())
                    .append(")");
            if (i != 100000) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }

    public String fillingGM() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT  `musicdatabase`.`gm`(`id_gm`,`id_musical_composition`,`id_genre`)VALUES");
        int[] rep = new int[100001];
        Arrays.fill(rep, 0);
        int v;
        for (int i = 1; i <= 250000; i++) {
            if (i > 100000) {
                do {
                    v = distributions.get("musicalComposition").sample();
                } while (rep[v] >= 5);
            } else {
                v = i;
            }
            query.append("(").append(i).append(",")
                    .append(v).append(",")
                    .append(distributions.get("genre").sample())
                    .append(")");
            rep[v]++;
            if (i != 250000) {
                query.append(",");
            }
        }
        query.append(";");
        return query.toString();
    }
}
