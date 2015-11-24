package wdb;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import wdb.metadata.*;
import wdb.parser.*;

import java.io.*;
import java.util.*;

/**
 * Created by Raymond on 11/16/2015.
 */
public class TitanWdbBasic {
    public static BufferedReader in = null;
    public static QueryParser parser = null;
    public static TitanGraph graph = null;
    public static TitanTransaction tx = null;
    private static Map<String, ClassDefNode> classDefs = new HashMap<>();

    public static void main(String[] args) {
        try {
            File dbDir = new File("db");
            if (dbDir.exists() && dbDir.isDirectory()) {
                FileUtils.deleteDirectory(dbDir);
            }
            List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
            loggers.add(LogManager.getRootLogger());
            for ( Logger logger : loggers ) {
                logger.setLevel(Level.OFF);
            }
            TitanFactory.Builder config = TitanFactory.build();
            config.set("storage.backend", "berkeleyje");
            config.set("storage.directory", "db/cannata");
            graph = config.open();
            TitanManagement mg = graph.openManagement();
            PropertyKey className = mg.makePropertyKey("class1234512345").dataType(String.class).make();
            VertexLabel label = mg.makeVertexLabel("entity").make();
            mg.buildIndex("byClassName", Vertex.class).addKey(className).indexOnly(label).buildCompositeIndex();
            mg.commit();
            tx = graph.newTransaction();

            System.out.println("WDB SIM on Titan Final Project");
            System.out.println("Implemented by: Alvin Deng and Raymond Chee");

            in = new BufferedReader(new InputStreamReader(System.in));
            parser = new QueryParser(in);

            Query q;

            while (true) {
                try {
                    if (!in.ready()) {
                        System.out.print("\nWDB>");
                    }

                    q = parser.getNextQuery();
                    if (q == null) {
                        break;
                    } else {
                        processQuery(q);
                    }
                } catch (ParseException pe) {
                    System.out.println("SYNTAX ERROR: " + pe.getMessage());
                    QueryParser.ReInit(System.in);
                } catch (TokenMgrError tme) {
                    System.out.println("PARSER ERROR: " + tme.getMessage());
                    break;
                } catch (IOException ioe) {
                    System.out.println("STANDARD IN ERROR: " + ioe.getMessage());
                    break;
                } catch (NumberFormatException nfe) {
                    System.out.println("PARSE ERROR: Failed to convert to Integer " + nfe.getMessage());
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            //System.out.println(e.getMessage());
        } finally {
            if (tx != null) {
                tx.close();
            }
            if (graph != null) {
                graph.close();
            }
        }
    }

    static private void processQuery(Query q) {
        if (q instanceof SourceQuery) {
            SourceQuery sq = (SourceQuery) q;
            processSourceQuery(sq);
        }

        if (q instanceof ClassDef) {
            ClassDef cd = (ClassDef) q;
            processClassDef(cd);
        }

        if (q instanceof ModifyQuery) {
            ModifyQuery mq = (ModifyQuery) q;
            processModifyQuery(mq);
        }

        if (q instanceof InsertQuery) {
            InsertQuery iq = (InsertQuery) q;
            processInsertQuery(iq);
        }
        if (q instanceof IndexDef) {
            IndexDef indexQ = (IndexDef) q;
            processIndexDef(indexQ);
        }
        if (q instanceof RetrieveQuery) {
            RetrieveQuery rq = (RetrieveQuery) q;
            processRetrieveQuery(rq);
        }
    }

    private static void processModifyQuery(ModifyQuery mq) {
        GraphTraversalSource g = graph.traversal();
        for (Vertex instance : getInstances(g, mq.className, mq.expression)) {
            doAssignments(g, instance, mq.className, mq);
        }
    }

    private static void processRetrieveQuery(RetrieveQuery rq) {
        if (tx != null) {
            tx.commit();
            tx = null;
            /*
            GraphTraversalSource g = graph.traversal();
            GraphTraversal<Vertex, Vertex> vertices = g.V();
            vertices.forEachRemaining(n -> {
                System.out.print("class " + n.property("class1234512345").value() + " ");
                n.edges(Direction.OUT).forEachRemaining(e -> {
                    System.out.print("-> " + e.label());
                });
                System.out.println();
            });
            System.exit(0);
            // */
        }
        GraphTraversalSource g = graph.traversal();
        for (Vertex instance : getInstancesRetrieve(g, rq.className, rq.expression)) {
            for (int j = 0; j < rq.numAttributePaths(); j++) {
                AttributePath path = rq.getAttributePath(j);
                Vertex v = instance;
                for (int i = 0; i < path.levelsOfIndirection(); i++) {
                    v = g.V(v.id()).out(path.getIndirection(i)).next();
                }
                if (path.attribute.equals("*")) {
                    Iterator<VertexProperty<Object>> it = v.properties();
                    while (it.hasNext()) {
                        VertexProperty<Object> property = it.next();
                        if (!property.key().equals("class1234512345")) {
                            System.out.print(property.key() + "->" + property.value() + (it.hasNext() ? " | " : "\n"));
                        }
                    }
                } else {
                    System.out.print(v.property(path.attribute).value() + (j == rq.numAttributePaths() - 1 ? "\n" : " | "));
                }
            }
        }
    }

    private static void processInsertQuery(InsertQuery iq) {
        GraphTraversalSource g = graph.traversal();
        try {
            int newVertexCount = 0, edgeCounts[] = new int[3];
            if (iq.fromClassName == null) {
                // just inserting a new entity
                Vertex newVertex = tx.addVertex(T.label, "entity", "class1234512345", iq.className);
                newVertexCount++;
                edgeCounts = doInsert(iq, g, newVertex, iq.className);
            } else {
                // inserting a subclass into an existing superclass
                for (Vertex instance : getInstances(g, iq.fromClassName, iq.expression)) {
                    instance.property("class1234512345", iq.className);
                    int[] arr = doInsert(iq, g, instance, iq.className);
                    for (int i = 0; i < 3; i++) {
                        edgeCounts[i] += arr[i];
                    }
                }
            }

            printUpdateQueryResults(newVertexCount, edgeCounts);
            System.out.println("Insert complete");
        } catch(RuntimeException e) {
            System.err.println("Insert failed!");
            throw e;
        }
    }

    private static Iterable<Vertex> getInstances(GraphTraversalSource g, String className, SimpleNode expression) {
        List<Vertex> list = new ArrayList<>();
        Iterable<TitanVertex> traversal = tx.getVertices();
        traversal.forEach(n -> {
            if (n.property("class1234512345").value().equals(className) && matches(expression, n)) {
                list.add(n);
            }
        });
        ClassDefNode cd = classDefs.get(className);
        if (cd == null) {
            System.out.println(className);
            System.exit(1);
        }
        for (String subclassName : cd.children) {
            getInstances(g, subclassName, expression).forEach(list::add);
        }
        return list;
    }

    private static Iterable<Vertex> getInstancesRetrieve(GraphTraversalSource g, String className, SimpleNode expression) {
        List<Vertex> list = new ArrayList<>();
        GraphTraversal<Vertex, Vertex> traversal = g.V().hasLabel("entity").has("class1234512345");
        traversal.forEachRemaining(n -> {
            if (n.property("class1234512345").value().equals(className) && matches(expression, n)) {
                list.add(n);
            }
        });
        ClassDefNode cd = classDefs.get(className);
        if (cd == null) {
            System.out.println(className);
            System.exit(1);
        }
        for (String subclassName : cd.children) {
            getInstancesRetrieve(g, subclassName, expression).forEach(list::add);
        }
        return list;
    }

    private static void printUpdateQueryResults(int newVertexCount, int[] edgeCounts) {
        // edgeCounts[0] = num replaced, edgeCounts[1] = num inserted, edgeCounts[2] = num removed
        if (newVertexCount != 0) {
            System.out.printf("%d vertices inserted.\n", newVertexCount);
        }
        if (edgeCounts[0] != 0) {
            System.out.printf("%d edges replaced.\n", edgeCounts[0]);
        }
        if (edgeCounts[1] != 0) {
            System.out.printf("%d edges inserted.\n", edgeCounts[1]);
        }
        if (edgeCounts[2] != 0) {
            System.out.printf("%d edges removed.\n", edgeCounts[2]);
        }
    }

    // returns the number of edges inserted
    private static int[] doInsert(InsertQuery iq, GraphTraversalSource g, Vertex entity, String className) {
        insertDefaults(className, entity);
        return doAssignments(g, entity, className, iq);
    }

    private static void insertDefaults(String className, Vertex entity) {
        ClassDefNode cd = classDefs.get(className);
        cd.defaults.keySet().stream().filter(dva -> !entity.property(dva).isPresent()).forEach(dva -> {
            entity.property(dva, cd.defaults.get(dva));
        });
    }

    // int[] data; data[0] = num edges replaced, data[1] = num edges inserted, data[2] = num edges removed
    private static int[] doAssignments(GraphTraversalSource g, Vertex entity, String className, UpdateQuery query) {
        int[] counts = new int[3];
        for (Assignment assignment : query.assignmentList) {
            if (assignment instanceof DvaAssignment) {
                DvaAssignment dvaAssignment = (DvaAssignment) assignment;
                entity.property(assignment.AttributeName, dvaAssignment.Value);
            } else if (assignment instanceof EvaAssignment) {
                EvaAssignment evaAssignment = (EvaAssignment) assignment;

                switch (evaAssignment.mode) {
                    case 0: { // REPLACE_MODE
                        // remove existing edges between entity and instances
                        Iterator<Edge> edges = entity.edges(Direction.OUT);
                        while(edges.hasNext()) {
                            edges.next();
                            edges.remove();
                            counts[0]++;
                        }
                        // continue on to insert
                    }
                    case 1: { // INSERT_MODE
                        Iterator<Vertex> targets = tx.vertices();
                        targets.forEachRemaining(v -> {
                            if (v.property("class1234512345").value().equals(evaAssignment.targetClass)) {
                                entity.addEdge(evaAssignment.AttributeName, v);
                                v.addEdge(getInverse(className, evaAssignment.AttributeName), entity);
                                counts[1]++;
                            }
                        });
                        break;
                    }
                    case 2: { // EXCLUDE_MODE
                        Iterator<Edge> edges = entity.edges(Direction.OUT);
                        while (edges.hasNext()) {
                            Edge e = edges.next();
                            if (e.label().equals(evaAssignment.AttributeName)) {
                                e.remove();
                                counts[2]++;
                            }
                        }
                    }
                }
            }
        }

        return counts;
    }

    private static String getInverse(String className, String attributeName) {
        ClassDefNode cd = classDefs.get(className);
        return cd.inverses.get(attributeName);
    }

    private static boolean matches(Node expression, Vertex instance) {
        if (expression == null) {
            return true;
        }
        if (expression instanceof Root) {
            return matches(expression.jjtGetChild(0), instance);
        }
        if (expression instanceof Cond) {
            String[] split = expression.toString().split(" ", 3);
            String attributeName = split[0];
            Property<Object> property = instance.property(attributeName);
            Object attribute = property.isPresent() ? property.value() : null;
            String quantifier = split[1];
            String value = split[2];
            switch (quantifier) {
                case "=" : {
                    return attribute == null && value.equals("NULL") ||
                            attribute != null && attribute.toString().equals(value);
                }
                case "<>" : {
                    return attribute == null && !value.equals("NULL") ||
                            attribute != null && !attribute.toString().equals(value);
                }
                case "<" : {
                    return attribute != null && ((Integer) attribute) < Integer.parseInt(value);
                }
                case ">" : {
                    return attribute != null && ((Integer) attribute) > Integer.parseInt(value);
                }
                case "<=" : {
                    return attribute != null && ((Integer) attribute) <= Integer.parseInt(value);
                }
                case ">=" : {
                    return attribute != null && ((Integer) attribute) >= Integer.parseInt(value);
                }
                default : {
                    throwException("Invalid quantifier %s!", quantifier);
                }
            }
        }
        if (expression instanceof And) {
            int numChildren = expression.jjtGetNumChildren();
            for (int i = 0; i < numChildren; i++) {
                if (!matches(expression.jjtGetChild(i), instance)) {
                    return false;
                }
            }
            return true;
        }
        if (expression instanceof Or) {
            int numChildren = expression.jjtGetNumChildren();
            for (int i = 0; i < numChildren; i++) {
                if (matches(expression.jjtGetChild(i), instance)) {
                    return true;
                }
            }
            return false;
        }
        if (expression instanceof Not) {
            return !matches(expression.jjtGetChild(0), instance);
        }
        if (expression instanceof False) {
            return false;
        }
        if (expression instanceof True) {
            return true;
        }
        throw new IllegalStateException("Unknown type of Node! " + expression);
    }

    private static void throwException(String format, Object... args) {
        throw new RuntimeException(String.format(format, args));
    }

    private static void processIndexDef(IndexDef indexQ) {
    }

    private static void processClassDef(ClassDef cd) {
        classDefs.put(cd.name, new ClassDefNode(cd));
    }

    private static void processSourceQuery(SourceQuery sq) {
        try {
            QueryParser.ReInit(new FileReader(sq.filename));
            Query fq;
            List<RuntimeException> sourceExceptions = new ArrayList<>();
            while (true) {
                try {
                    fq = parser.getNextQuery();
                    if (fq == null) {
                        break;
                    } else {
                        processQuery(fq);
                    }
                } catch(RuntimeException e) {
                    sourceExceptions.add(e);
                }
            }
            if (!sourceExceptions.isEmpty()) {
                System.out.printf("Sourcing %s resulted in %d errors!\n", sq.filename, sourceExceptions.size());
                for (RuntimeException e : sourceExceptions) {
                    System.out.println(e.getMessage());
                    e.printStackTrace(System.err);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("FILE OPEN ERROR: " + e.getMessage());
        } catch (ParseException pe) {
            System.out.println("SYNTAX ERROR: " + pe.getMessage());
        } catch (TokenMgrError tme) {
            System.out.println("PARSER ERROR: " + tme.getMessage());
        } finally {
            QueryParser.ReInit(in);
        }
    }

    private static class ClassDefNode {
        List<String> parents;
        List<String> children;
        String className;
        Map<String, Object> defaults;
        Map<String, String> inverses;

        public ClassDefNode(ClassDef cd) {
            parents = new ArrayList<>();
            children = new ArrayList<>();
            defaults = new HashMap<>();
            inverses = new HashMap<>();
            className = cd.name;
            for (int i = 0; i < cd.numberOfAttributes(); i++) {
                Attribute attr = cd.getAttribute(i);
                if (attr instanceof DVA) {
                    DVA dva = (DVA) attr;
                    if (dva.initialValue != null) {
                        defaults.put(dva.name, dva.initialValue);
                    }
                } else if (attr instanceof EVA) {
                    EVA eva = (EVA) attr;
                    inverses.put(eva.name, eva.inverseEVA);
                    if (classDefs.containsKey(eva.baseClassName)) {
                        ClassDefNode other = classDefs.get(eva.baseClassName);
                        other.inverses.put(eva.inverseEVA, eva.name);
                    }
                }
            }

            if (cd instanceof SubclassDef) {
                SubclassDef scd = (SubclassDef)cd;
                for (int i = 0; i < scd.numberOfSuperClasses(); i++) {
                    String sup = scd.getSuperClass(i);
                    parents.add(sup);
                    classDefs.get(sup).children.add(className);
                }
            }
        }
    }
}
