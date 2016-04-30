package wdb;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import wdb.metadata.*;
import wdb.parser.*;

import java.io.*;
import java.util.*;

/**
 * WDB + TitanDB
 *
 * Created by Raymond Chee and Alvin Deng on 11/16/2015.
 *
 * Updated by Alvin Deng on 04/30/2016.
 */

public class TitanWDB {
    private static BufferedReader in = null;
    private static QueryParser parser = null;
    private static TitanGraph graph = null;
    private static Object rootID = null;

    public static void main(String[] args) {
        try {
            initTitanDB();
            processInput();

        } catch (RuntimeException e) {
            throw e;
        }
    }

    private static void initTitanDB() {
        TitanFactory.Builder config = TitanFactory.build();
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", "db/CarnotKE");
        graph = config.open();
        TitanManagement mg = graph.openManagement();
        boolean initGraph = mg.getGraphIndex("byClassDef") == null;
        if (initGraph) {
            VertexLabel classLabel = mg.makeVertexLabel("classDef").make();
            PropertyKey name = mg.makePropertyKey("name").dataType(String.class).make();
            mg.buildIndex("byClassDef", Vertex.class).addKey(name).indexOnly(classLabel).buildCompositeIndex();
        }
        mg.commit();
        if (initGraph) {
            Vertex root = graph.addVertex(T.label, "classDef", "name", "root node");
            graph.tx().commit();
            rootID = root.id();
        } else {
            GraphTraversalSource g = graph.traversal();
            g.V().hasLabel("classDef").forEachRemaining(n -> {
                if (n.property("name").value().equals("root node")) {
                    rootID = n.id();
                }
            });
        }
    }
    private static void processInput() {
        System.out.println("WDB + TitanDB 2.0");
        System.out.println("Implemented by: Alvin Deng");
        in = new BufferedReader(new InputStreamReader(System.in));
        parser = new QueryParser(in);
        Query q;
        while (true) {
            try {
                if (!in.ready()) {
                    System.out.print("\nWDB >");
                }

                q = parser.getNextQuery();
                if (q == null) {
                    break;
                } else {
                    processQuery(q);
                }
            } catch (ParseException pe) {
                System.out.println("Syntax Error: " + pe.getMessage());
                QueryParser.ReInit(System.in);
            } catch (TokenMgrError tme) {
                System.out.println("Parser Error: " + tme.getMessage());
                break;
            } catch (IOException ioe) {
                System.out.println("Standard Input Error: " + ioe.getMessage());
                break;
            } catch (NumberFormatException nfe) {
                System.out.println("Parse Error: Failed to convert to Integer " + nfe.getMessage());
            }
        }
    }

    private static void processQuery(Query q) {
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

    private static Vertex getVertex(String classDef, String attribute, String value) {
        Iterator<Vertex> vertices = graph.vertices();

        while (vertices.hasNext()) {
            Vertex nextV = vertices.next();
            String currentLabel = nextV.label();
            if (currentLabel.equals("entity") && nextV.property("class").value().equals(classDef)) {
                if (nextV.property(attribute).value().toString().equals(value)) {
                    return nextV;
                }
            }
        }
        return null;
    }

    private static String findInverseEdgeName(String classDefName, String edgeName) {
        Iterator<Vertex> iter = graph.vertices();
        while (iter.hasNext()) {
            Vertex currentVertex = iter.next();
            if (currentVertex.label().equals("attribute")) {
                if (currentVertex.property("name").value().equals(edgeName)) {
                    Iterator<Edge> edges = currentVertex.edges(Direction.OUT);
                    while (edges.hasNext()) {
                        Edge currentE = edges.next();
                        String inverseEdgeName = currentE.inVertex().property("name").value().toString();
                        if (currentE.inVertex().property("class").value().equals(classDefName)) {
                            return inverseEdgeName;
                        }
                    }
                }
            }
        }
        return null;
    }

    private static void processModifyQuery(ModifyQuery mq) {
        GraphTraversalSource g = graph.traversal();
        ArrayList<Vertex> instanceList = new ArrayList<>();
        Iterator<Vertex> vertices = graph.vertices();
        ArrayList<Vertex> modifyingVertex = new ArrayList<>();

        while (vertices.hasNext()) {
            Vertex nextV = vertices.next();
            String currentLabel = nextV.label();
            if (currentLabel.equals("entity") && nextV.property("class").value().equals(mq.className)) {
                instanceList.add(nextV);
            }
        }

        Vertex classDef = lookupClass(g, mq.className);
        for (Vertex instance : instanceList) {
            if (matches(g, classDef, mq.expression, instance)) {
                modifyingVertex.add(instance);
            }
        }

        for (Vertex startVertex : modifyingVertex) {
            for (Assignment y : mq.assignmentList) {
                EvaAssignment current = (EvaAssignment) y;
                String[] targetArray = current.expression.jjtGetChild(0).toString().split(" ");
                String targetAttribute = targetArray[0];
                String targetValue = targetArray[2];
                Vertex targetVertex = getVertex(current.targetClass, targetAttribute, targetValue);
                String edgeName = y.AttributeName;
                startVertex.addEdge(edgeName, targetVertex);

                String inverseEdgeName = findInverseEdgeName(mq.className, edgeName);
                assert targetVertex != null;
                targetVertex.addEdge(inverseEdgeName, startVertex);
            }


        }

        graph.tx().commit();
    }

    private static void debugInstances() {
        System.out.println("debugInstances\n");
        GraphTraversalSource g = graph.traversal();
        g.V().hasLabel("entity").has("name").forEachRemaining(n -> {
            System.out.println("Entity: " + n.property("name").value());
            g.V(n.id()).properties().forEachRemaining(m -> System.out.println(m.key() + " " + m.value()));
            System.out.println();
        });
    }

    private static void debugAttributes() {
        System.out.println("debugAttributes\n");
        GraphTraversalSource g = graph.traversal();
        g.V().hasLabel("attribute").has("name").forEachRemaining(n -> {
            System.out.println("Attribute: " + n.property("name").value());
            g.V(n.id()).properties().forEachRemaining(m -> System.out.println(m.key() + " " + m.value()));
            System.out.println();
        });
    }

    private static void debugClassDefs() {
        System.out.println("debugClassDefs\n");
        GraphTraversalSource g = graph.traversal();
        g.V().hasLabel("classDef").has("name").forEachRemaining(n -> {
            System.out.println("classDef: " + n.property("name").value());
            g.V(n.id()).outE().forEachRemaining(m -> System.out.println("Keys: " + m.keys() + " " + m.inVertex().property("name").value()));
            System.out.println();
        });
    }


    private static void recurRetrieve(String classDef, ArrayList<Vertex> overall) {
        Iterator<Vertex> vertices = graph.vertices();
        HashSet<String> children = new HashSet<>();

        while (vertices.hasNext()) {
            Vertex nextV = vertices.next();
            String currentLabel = nextV.label();
            if (currentLabel.equals("classDef")) {
                Iterator<Edge> iter = nextV.edges(Direction.OUT);
                while (iter.hasNext()) {
                    Edge currentE = iter.next();
                    if (currentE.label().equals("subclasses")) {
                        String inbound = currentE.inVertex().property("name").value().toString();
                        if (inbound.equals(classDef)) {
                            children.add(currentE.outVertex().property("name").value().toString());
                        }
                    }
                }
            }
            if (currentLabel.equals("entity") && nextV.property("class").value().equals(classDef)) {
                overall.add(nextV);
            }
        }

        for (String x : children) {
            recurRetrieve(x, overall);
        }
    }

    private static void processRetrieveQuery(RetrieveQuery rq) {
        ArrayList<Vertex> overall = new ArrayList<>();

        recurRetrieve(rq.className, overall);

        for (Vertex instance : overall) {
            for (int j = 0; j < rq.numAttributePaths(); j++) {
                AttributePath path = rq.getAttributePath(j);
                HashMap<Vertex, String> neighborMap = new HashMap<>();

                for (int i = 0; i < path.levelsOfIndirection(); i++) {
                    Iterator<Edge> iter = instance.edges(Direction.OUT);
                    while (iter.hasNext()) {
                        Edge current = iter.next();
                        if (current.label().equals(path.getIndirection(i))) {
                            if (!neighborMap.containsKey(current.inVertex())) {
                                neighborMap.put(current.inVertex(), path.getIndirection(i));
                            }
                        }
                    }
                }

                if (path.attribute.equals("*")) {
                    Iterator<VertexProperty<Object>> it = instance.properties();
                    while (it.hasNext()) {
                        VertexProperty<Object> property = it.next();
                        if (!property.key().equals("property")) {
                            System.out.print(property.key() + "->" + property.value() + (it.hasNext() ? " | " : "\n"));
                        }
                    }
                } else {
                    for (Vertex current : neighborMap.keySet()) {
                        System.out.print(neighborMap.get(current) + "_" + path.attribute + "->" + current.property(path.attribute).value() + (j == rq.numAttributePaths() - 1 ? "\n" : " | "));
                    }
                }
            }
        }
    }

    private static void processInsertQuery(InsertQuery iq) {
        GraphTraversalSource g = graph.traversal();
        try {
            int newVertexCount = 0, edgeCounts[];
            Vertex classDef = lookupClass(g, iq.className);
            if (classDef == null) {
                throwException("Cannot insert into class %s because it does not exist!", iq.fromClassName);
            }
            if (iq.fromClassName == null) {
                // just inserting a new entity
                Vertex entity = graph.addVertex(T.label, "entity", "class", iq.className);
                newVertexCount++;
                // make the entity an instance of the class
                classDef.addEdge("instance", entity);
                edgeCounts = doInsert(iq, g, classDef, entity);
            } else {
                // inserting a subclass into an existing superclass
                Vertex superclassDef = lookupClass(g, iq.fromClassName);
                if (superclassDef == null) {
                    throwException("Cannot extend into class %s because it does not exist!", iq.fromClassName);
                }
                if (!isSubclass(g, superclassDef, iq.className)) {
                    throwException("Cannot extend class %s to class %s because %2$s is not a subclass of %1$s!",
                            iq.fromClassName, iq.className);
                }
                edgeCounts = new int[3];
                for (Vertex instance : getInstances(g, superclassDef, iq.expression)) {
                    g.V(instance.id()).inE("instance").next().remove();
                    classDef.addEdge("instance", instance);
                    int[] counts = doInsert(iq, g, classDef, instance);
                    for (int i = 0; i < 3; i++) {
                        edgeCounts[i] += counts[i];
                    }
                }
            }

            printUpdateQueryResults(newVertexCount, edgeCounts);
            graph.tx().commit();
        } catch (RuntimeException e) {
            System.err.println("Insert failed! Rolling back changes.");
            graph.tx().rollback();
            throw e;
        }
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
    private static int[] doInsert(InsertQuery iq, GraphTraversalSource g, Vertex classDef, Vertex entity) {
        setDefaultDVAs(g, classDef, entity);
        int[] counts = doAssignments(g, entity, classDef, iq);
        checkRequiredInserts(iq, g, classDef, entity);
//        checkEVARestrictions(iq, g, classDef, entity);
        return counts;
    }

    private static void setDefaultDVAs(GraphTraversalSource g, Vertex classDef, Vertex entity) {
        GraphTraversal<Vertex, Vertex> dvas = g.V(classDef.id()).outE("has").has("isDVA").inV().has("default_value");
        while (dvas.hasNext()) {
            Vertex dvaVertex = dvas.next();
            Object initValue = dvaVertex.property("default_value").value();
            String dvaName = (String) dvaVertex.property("name").value();
            if (!entity.property(dvaName).isPresent()) {
                entity.property(dvaName, initValue);
            }
        }
    }

    private static void checkRequiredInserts(InsertQuery iq, GraphTraversalSource g, Vertex classDef, Vertex entity) {
        GraphTraversal<Vertex, Edge> requiredAttrs = g.V(classDef.id()).outE("has").has("required");
        while (requiredAttrs.hasNext()) {
            Edge attr = requiredAttrs.next();
            Vertex required = attr.inVertex();
            String name = (String) required.property("name").value();
            if (attr.property("isDVA").isPresent()) {
                // didn't insert a required DVA
                if (!entity.property(name).isPresent()) {
                    throwException("Did not insert required value %s into an instance of class %s!",
                            name, iq.className);
                }
            } else {
                // didn't insert a required EVA
                if (!g.V(entity.id()).outE(name).hasNext()) {
                    throwException("Did not insert required EVA %s into an instance of class %s!",
                            name, iq.className);
                }
            }
        }
    }

    private static Vertex getInverseEVA(Vertex classDef, String evaVertexName) {
        Iterator<Edge> iter = classDef.edges(Direction.OUT);
        while(iter.hasNext()) {
            Edge currentE = iter.next();
            if(currentE.label().equals("has")) {
                if(currentE.inVertex().property("name").value().toString().equals(evaVertexName)) {
                    return currentE.inVertex();
                }
            }
        }
        return null;
    }

    // check if the insertion respects max number of references defined and distinctness restriction
    private static void checkEVARestrictions(InsertQuery iq, GraphTraversalSource g, Vertex classDef, Vertex entity) {
        GraphTraversal<Vertex, Vertex> evaAttrs = g.V(classDef.id()).outE("has").hasNot("isDVA").inV();
        while (evaAttrs.hasNext()) {

            Vertex evaVertex = evaAttrs.next();

            /* Inverse Lookup */
            Vertex inverseEVA = getInverseEVA(classDef, evaVertex.property("name").value().toString());

            boolean singleValued = evaVertex.property("isSV").value().toString().equals("true");
            assert inverseEVA != null;
            boolean inverseSingleValued = inverseEVA.property("isSV").value().toString().equals("true");
            int maxReferences = singleValued ? 1 : (Integer) evaVertex.property("max").value();
            int inverseMaxReferences = inverseSingleValued ? 1 : (Integer) inverseEVA.property("max").value();
            boolean checkDistinct = !singleValued && evaVertex.property("distinct").value().toString().equals("true");
            boolean inverseCheckDistinct = !inverseSingleValued && inverseEVA.property("distinct").value().toString().equals("true");
            Set<Object> connectedVertexIds = new HashSet<>();
            String evaName = evaVertex.property("name").value().toString();
            String inverseName = inverseEVA.property("name").value().toString();
            GraphTraversal<Vertex, Vertex> connections = g.V(entity.id()).out(evaName);
            int count = 0;

            while (connections.hasNext()) {
                Vertex connection = connections.next();
                if (checkDistinct && !connectedVertexIds.add(connection.id())) {
                    // we need to check for distinctness and connected vertices failed to add
                    // (and thus we have a duplicate)
                    throwException("EVA %s is DISTINCT but class %s entity %s " +
                                    "references class %s entity %s more than once!",
                            evaName, iq.className, entity.id(),
                            evaVertex.property("class").value(), connection.id());
                }
                if (++count > maxReferences) {
                    throwException("EVA %s has a limit of %d references which was exceeded!",
                            evaName, maxReferences);
                }
                int inverseCount = 0;
                GraphTraversal<Vertex, Vertex> inverses = g.V(connection.id()).out(inverseName);
                while (inverses.hasNext()) {
                    if (inverseCheckDistinct && inverseCount > 0) {
                        throwException("Eva %s is DISTINCT but class %s entity %s " +
                                        "references class %s entity %s more than once!",
                                inverseName, evaVertex.property("class").value(), connection.id(),
                                iq.className, entity.id());
                    }
                    if (++inverseCount > inverseMaxReferences) {
                        throwException("EVA %s has a limit of %d references which was exceeded!",
                                inverseName, inverseMaxReferences);
                    }
                }
            }
        }
    }

    // int[] data; data[0] = num edges replaced, data[1] = num edges inserted, data[2] = num edges removed
    private static int[] doAssignments(GraphTraversalSource g, Vertex entity, Vertex classDef, UpdateQuery query) {
        int[] counts = new int[3];
        for (Assignment assignment : query.assignmentList) {
            if (assignment instanceof DvaAssignment) {
                Vertex dvaVertex = getAttribute(g, classDef, true, assignment.AttributeName);
                if (dvaVertex == null) {
                    throwException("Class %s does not have an dva %s!", query.className, assignment.AttributeName);
                }
                DvaAssignment dvaAssignment = (DvaAssignment) assignment;
                checkAssignmentType(query, assignment.AttributeName, dvaVertex, dvaAssignment);
                entity.property(assignment.AttributeName, dvaAssignment.Value);
            } else if (assignment instanceof EvaAssignment) {
                Vertex evaVertex = getAttribute(g, classDef, false, assignment.AttributeName);
                if (evaVertex == null) {
                    throwException("Class %s does not have an eva %s!", query.className, assignment.AttributeName);
                }
                EvaAssignment evaAssignment = (EvaAssignment) assignment;
                String evaClass = (String) evaVertex.property("class").value();
                if (!evaClass.equals(evaAssignment.targetClass) &&
                        !isSubclass(g, lookupClass(g, evaClass), evaAssignment.targetClass)) {
                    throwException("EVA %s cannot be assigned from class %s to class %s!",
                            assignment.AttributeName, query.className, evaClass);
                }
                System.out.println("evaClassDef: " + evaClass);
                Vertex evaClassDef = lookupClass(g, evaClass);
                if (evaClassDef == null) {
                    throwException("Class %s is not defined!", evaClass);
                }
                System.out.println("Getting Instances");
                Set<Vertex> instances = getInstances(g, evaClassDef, query.expression);
                String evaInverseName = (String) g.V(evaClassDef.id()).out("inverse").next().property("name").value();
                switch (evaAssignment.mode) {
                    case 0: { // REPLACE_MODE
                        // remove existing edges between entity and instances
                        for (Vertex instance : instances) {
                            // may want to remove at the end in case it messes up graph traversals
                            List<Edge> toRemove = new ArrayList<>();
                            scheduleDisconnect(g, entity, instance, toRemove);
                            scheduleDisconnect(g, instance, entity, toRemove);
                            for (Edge e : toRemove) {
                                e.remove();
                                counts[0]++;
                            }
                        }
                        // continue on to insert
                    }
                    case 1: { // INSERT_MODE
                        for (Vertex instance : instances) {
                            entity.addEdge("eva", instance, "name", evaAssignment.AttributeName);
                            instance.addEdge("eva", entity, "name", evaInverseName);
                            counts[1]++;
                        }
                        break;
                    }
                    case 2: { // EXCLUDE_MODE
                        for (Vertex instance : instances) {
                            GraphTraversal<Vertex, Edge> evas =
                                    g.V(instance.id()).outE("eva").has("name");
                            while (evas.hasNext()) {
                                Edge edge = evas.next();
                                if (edge.property("name").value().equals(evaAssignment.AttributeName)) {
                                    GraphTraversal<Edge, Edge> connected = g.E(edge.id()).inV().outE("eva").has("name");
                                    List<Edge> toRemove = new ArrayList<>();
                                    toRemove.add(edge);
                                    while (connected.hasNext()) {
                                        Edge next = connected.next();
                                        if (next.property("name").value().equals(evaInverseName)) {
                                            toRemove.add(next);
                                        }
                                    }
                                    for (Edge e : toRemove) {
                                        e.remove();
                                        counts[2]++;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return counts;
    }

    private static void scheduleDisconnect(GraphTraversalSource g, Vertex start, Vertex end, List<Edge> toRemove) {
        GraphTraversal<Vertex, Edge> forwards = g.V(start.id()).outE("eva");
        while (forwards.hasNext()) {
            Edge e = forwards.next();
            if (e.inVertex().id().equals(end.id())) {
                toRemove.add(e);
            }
        }
    }

    private static Set<Vertex> getInstances(GraphTraversalSource g, Vertex classDef, SimpleNode expression) {
        Set<Vertex> res = new HashSet<>();
        GraphTraversal<Vertex, Vertex> instances = g.V(classDef.id()).out("instance");
        while (instances.hasNext()) {
            Vertex instance = instances.next();
            System.out.println("getInstances Instance: " + instance.property("name").value());
            if (matches(g, classDef, expression, instance)) {
                res.add(instance);
            }
        }

        GraphTraversal<Vertex, Vertex> subclasses = g.V(classDef.id()).out("superclasses");
        while (subclasses.hasNext()) {
            System.out.println("yes");
            res.addAll(getInstances(g, subclasses.next(), expression));
        }
        return res;
    }

    private static boolean matches(GraphTraversalSource g, Vertex classDef, Node expression, Vertex instance) {
        if (expression instanceof Root) {
            return matches(g, classDef, expression.jjtGetChild(0), instance);
        }
        if (expression instanceof Cond) {
            String[] split = expression.toString().split(" ", 3);
            String attributeName = split[0];
            Vertex attrVertex = getAttribute(g, classDef, true, attributeName);
//            System.out.println("Condition Attribute Name: " + attributeName);
//            System.out.println("Condition classDef Name: " + classDef.property("name").value());
            if (attrVertex == null) {
                throwException("Class %s doesn't have attribute %s!", classDef.property("name"), attributeName);
            }
            Property<Object> property = instance.property(attributeName);
            Object attribute = property.isPresent() ? property.value() : null;
            String quantifier = split[1];
            String value = split[2];
            switch (quantifier) {
                case "=": {
                    return attribute == null && value.equals("NULL") ||
                            attribute != null && attribute.toString().equals(value);
                }
                case "<>": {
                    return attribute == null && !value.equals("NULL") ||
                            attribute != null && !attribute.toString().equals(value);
                }
                case "<": {
                    checkAttributeIsInteger(attributeName, attrVertex, quantifier);
                    return attribute != null && ((Integer) attribute) < Integer.parseInt(value);
                }
                case ">": {
                    checkAttributeIsInteger(attributeName, attrVertex, quantifier);
                    return attribute != null && ((Integer) attribute) > Integer.parseInt(value);
                }
                case "<=": {
                    checkAttributeIsInteger(attributeName, attrVertex, quantifier);
                    return attribute != null && ((Integer) attribute) <= Integer.parseInt(value);
                }
                case ">=": {
                    checkAttributeIsInteger(attributeName, attrVertex, quantifier);
                    return attribute != null && ((Integer) attribute) >= Integer.parseInt(value);
                }
                default: {
                    throwException("Invalid quantifier %s!", quantifier);
                }
            }
        }
        if (expression instanceof And) {
            int numChildren = expression.jjtGetNumChildren();
            for (int i = 0; i < numChildren; i++) {
                if (!matches(g, classDef, expression.jjtGetChild(i), instance)) {
                    return false;
                }
            }
            return true;
        }
        if (expression instanceof Or) {
            int numChildren = expression.jjtGetNumChildren();
            for (int i = 0; i < numChildren; i++) {
                if (matches(g, classDef, expression.jjtGetChild(i), instance)) {
                    return true;
                }
            }
            return false;
        }
        if (expression instanceof Not) {
            return !matches(g, classDef, expression.jjtGetChild(0), instance);
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

    private static void checkAttributeIsInteger(String attributeName, Vertex attrVertex, String quantifier) {
        String data_type = (String) attrVertex.property("data_type").value();
        if (!data_type.equals("integer")) {
            throwException("Cannot compare attribute %s of type %s with quantifier '%s'",
                    attributeName, data_type, quantifier);
        }
    }

    private static Vertex getSubclass(GraphTraversalSource g, Object classID, String targetClassName) {
        GraphTraversal<Vertex, Vertex> subclasses = g.V(classID).out("superclasses");
        while (subclasses.hasNext()) {
            Vertex subclass = subclasses.next();
            String subclassName = (String) subclass.property("name").value();
            Vertex temp;
            if (subclassName.equals(targetClassName)) {
                return subclass;
            } else if ((temp = getSubclass(g, subclass, targetClassName)) != null) {
                return temp;
            }
        }
        return null;
    }

    private static boolean isSubclass(GraphTraversalSource g, Vertex classDef, String targetClassName) {
        return getSubclass(g, classDef.id(), targetClassName) != null;
    }

    private static void checkAssignmentType(UpdateQuery query, String name, Vertex attrVertex, DvaAssignment dvaAssignment) {
        String type = (String) attrVertex.property("data_type").value();
        switch (type) {
            case "boolean": {
                if (!(dvaAssignment.Value instanceof Boolean)) {
                    throwException("Attribute %s of class %s stores boolean values! Found %s",
                            name, query.className, dvaAssignment.Value);
                }
                break;
            }
            case "integer": {
                if (!(dvaAssignment.Value instanceof Integer)) {
                    throwException("Attribute %s of class %s stores integer values! Found %s",
                            name, query.className, dvaAssignment.Value);
                }
                break;
            }
            case "string": {
                if (!(dvaAssignment.Value instanceof String)) {
                    throwException("Attribute %s of class %s stores string values! Found %s",
                            name, query.className, dvaAssignment.Value);
                }
                break;
            }
        }
    }

    private static Vertex getAttribute(GraphTraversalSource g, Vertex classDef, boolean isDVA, String attributeName) {
        GraphTraversal<Vertex, Vertex> attrs;
        if (isDVA) {
            attrs = g.V(classDef.id()).outE("has").has("isDVA").inV().has("name");
        } else {
            attrs = g.V(classDef.id()).outE("has").hasNot("isDVA").inV().has("name");
        }
        while (attrs.hasNext()) {
            Vertex v = attrs.next();
            if (v.property("name").isPresent() && v.property("name").value().equals(attributeName)) {
                return v;
            }
        }
        return null;
    }

    private static void processIndexDef(IndexDef indexQ) {
        throw new UnsupportedOperationException();
    }

    private static void processClassDef(ClassDef cd) {
        GraphTraversalSource g = graph.traversal();
        try {
            cd.name = cd.name.toLowerCase();
            TitanVertex currentVertex = (TitanVertex) lookupClass(g, cd.name);
            if ((currentVertex != null && !currentVertex.property("ForwardInit").isPresent()) || (currentVertex != null && currentVertex.property("ForwardInit").value().equals("No"))) {
                throwException("Class %s already exists!\n", cd.name);
            }
            TitanVertex newClass;
            if (currentVertex == null) {
                newClass = graph.addVertex(T.label, "classDef", "name", cd.name);
            } else {
                currentVertex.property("ForwardInit", "No");
                newClass = currentVertex;
            }

            if (cd.comment != null) {
                newClass.property("comment", cd.comment);
            }
            for (int i = 0; i < cd.numberOfAttributes(); i++) {
                Attribute attr = cd.getAttribute(i);
                attr.name = attr.name.toLowerCase();
                TitanVertex attrVertex = graph.addVertex(T.label, "attribute", "name", attr.name);
                if (attr.comment != null) {
                    attrVertex.property("comment", attr.comment);
                }

                boolean required = attr.required != null && attr.required;
                Edge attrEdge = newClass.addEdge("has", attrVertex);
                if (required) {
                    attrEdge.property("required", true);
                }

                if (attr instanceof DVA) /* Simple Attribute */ {
                    attrEdge.property("isDVA", true);
                    DVA dva = (DVA) attr;
                    attrVertex.property("data_type", dva.type.toLowerCase());
                    if (dva.initialValue != null) {
                        attrVertex.property("default_value", dva.initialValue);
                    }
                } else if (attr instanceof EVA) /* This is a classDef */ {
                    EVA eva = (EVA) attr;
                    eva.baseClassName = eva.baseClassName.toLowerCase();
                    eva.inverseEVA = eva.inverseEVA.toLowerCase();
                    Vertex targetClass = lookupClass(g, eva.baseClassName);

                    if (targetClass == null) {
                        targetClass = graph.addVertex(T.label, "classDef", "name", eva.baseClassName);
                        targetClass.property("ForwardInit", "Yes");

                        GraphTraversal<Vertex, Vertex> traversal = g.V().hasLabel("classDef").has("name", "root node");
                        Vertex root = traversal.next();
                        targetClass.addEdge("subclasses", root);
                        root.addEdge("superclasses", targetClass);
//                        throwException("Class %s cannot create a relationship with %s because %2$s does not exist!",
//                                cd.name, eva.baseClassName);
                    }

                    attrVertex.property("class", eva.baseClassName, "isSV", eva.cardinality.equals(EVA.SINGLEVALUED));
                    TitanVertex inverseVertex = graph.addVertex(T.label, "attribute",
                            "name", eva.inverseEVA, "class", cd.name, "isSV", true);
                    if (attr.comment != null) {
                        inverseVertex.property("comment", attr.comment);
                    }
                    Edge inverseEdge = targetClass.addEdge("has", inverseVertex);
                    if (required) {
                        inverseEdge.property("required", true);
                    }

                    attrVertex.addEdge("inverse", inverseVertex);
                    inverseVertex.addEdge("inverse", attrVertex);

                    if (eva.distinct != null) {
                        attrVertex.property("distinct", eva.distinct);
                    }
                    if (eva.max != null) {
                        attrVertex.property("max", eva.max);
                    }

                    graph.tx().commit();

                } else {
                    throwException("Attribute %s is not a DVA or an EVA!", attr);
                }
            }

            if (cd instanceof SubclassDef) {
                SubclassDef scd = (SubclassDef) cd;
                for (int i = 0; i < scd.numberOfSuperClasses(); i++) {
                    String superClassName = scd.getSuperClass(i);
                    if (superClassName.equals(cd.name)) {
                        throwException("Class %s cannot subclass itself!", cd.name);
                    }
                    Vertex superClass = lookupClass(g, superClassName);
                    if (superClass != null) {
                        superClass.addEdge("superclasses", newClass);
                        newClass.addEdge("subclasses", superClass);

                        // copy attributes from superclass
                        GraphTraversal<Vertex, Edge> superAttributes = g.V(superClass.id()).outE("has");
                        while (superAttributes.hasNext()) {
                            Edge edge = superAttributes.next();
                            Edge e = newClass.addEdge("has", edge.inVertex());
                            if (edge.property("isDVA").isPresent()) {
                                e.property("isDVA", true);
                            }
                            if (edge.property("required").isPresent()) {
                                e.property("required", true);
                            }
                        }
                    } else {
                        throwException("Class %s subclasses the non-existent %s class!",
                                cd.name, superClassName);
                    }
                }
            } else {
                GraphTraversal<Vertex, Vertex> traversal = g.V().hasLabel("classDef").has("name", "root node");
                Vertex root = traversal.next();

                boolean found = false;
                Iterator<Edge> iter = newClass.edges(Direction.IN);
                while (iter.hasNext()) {
                    Edge x = iter.next();
                    if (x.outVertex().property("name").value().equals("root node")) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    newClass.addEdge("subclasses", root);
                    root.addEdge("superclasses", newClass);
                }
            }

            graph.tx().commit();
            System.out.printf("Class %s defined\n", cd.name);
        } catch (RuntimeException e) {
            graph.tx().rollback();
            throw e;
        }
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
                } catch (RuntimeException e) {
                    sourceExceptions.add(e);
                }
            }
            if (!sourceExceptions.isEmpty()) {
                System.out.printf("Sourcing %s resulted in %d errors!\n", sq.filename, sourceExceptions.size());
                for (RuntimeException e : sourceExceptions) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("File Open Error: " + e.getMessage());
        } catch (ParseException pe) {
            System.out.println("Syntax Error: " + pe.getMessage());
        } catch (TokenMgrError tme) {
            System.out.println("Parser Error: " + tme.getMessage());
        } finally {
            QueryParser.ReInit(in);
        }
    }

    private static Vertex lookupClass(GraphTraversalSource g, String name) {
        return getSubclass(g, rootID, name);
    }
}