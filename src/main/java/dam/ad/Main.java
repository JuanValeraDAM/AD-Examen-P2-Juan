package dam.ad;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.basex.api.client.ClientQuery;
import org.basex.api.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/*
TODO: En algún lugar hay un error porque el descuento a aplicar no lo está calculando bien.
 */
public class Main {
    public static void main(String[] args) throws SQLException, IOException {

        Logger.getLogger("org.mongodb").setLevel(Level.WARNING);
        try (Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "adp2", "bla");
             ClientSession baseX = new ClientSession("localhost", 1984, "admin", "admin");
             MongoClient mongo = MongoClients.create()) {
            oracle.setAutoCommit(false);

            //1. (máx. 0,50 puntos) Borrar todos los documentos de la colección “compras” de
            //MongoDB, visualizando cuántos se han borrado (inicialmente no habrá ninguno).
            System.out.println("1");

            MongoCollection<Document> col = mongo.getDatabase("test").getCollection("compras");
            System.out.println("Se han borrado " + col.deleteMany(new Document()).getDeletedCount());


            //2. (máx. 0,50 puntos) Crear una base de datos “clientes” en BaseX, usando “clientes.xml”
            //como documento inicial.
            System.out.println("2");

            baseX.create("clientes", new FileInputStream("clientes.xml"));

            //3. (máx. 2,00 puntos) Por cada emisor de la base de datos XML “clientes”, obtener el
            //importe de cada compra realizada por él. Puedes usar la consulta
            //distinct-values(//emisor) para obtener los emisores, y luego obtener los importes con
            ////cliente[tarjeta/emisor='EMISOR']/compras/compra/@importe/number(),
            //sustituyendo EMISOR por el emisor que toque a cada paso.
            System.out.println("3");

            List<Document> docsEmisores = new ArrayList<>();
            ClientQuery queryEmisores = baseX.query("distinct-values(//emisor)");
            queryEmisores.execute();
            while (queryEmisores.more()) {
                String emisor = queryEmisores.next();
                ClientQuery queryCompras = baseX.query(String.format("//cliente[tarjeta/emisor='%s']/compras/compra/@importe/number()", emisor));
                queryCompras.execute();
                while (queryCompras.more()) {
                    Double importeCompra = Double.parseDouble(queryCompras.next());
                    Document doc = new Document("emisor", emisor).append("importe", importeCompra);
                    docsEmisores.add(doc);
                }
            }
            // (máx. 1,00 punto) Por cada compra individual obtenida en el paso anterior, insertar un
            //documento en la colección “compras” de MongoDB que contenga el emisor y el
            //importe de la compra. Se valorará que haga en una sola operación de inserción.
            System.out.println("4");

            col.insertMany(docsEmisores);

            //5. (máx. 0,50 puntos) Invocar el procedimiento “limpiar” de Oracle.
            System.out.println("5");

            CallableStatement csLimpiar = oracle.prepareCall("{call limpiar()}");
            csLimpiar.execute();

            //6. (máx. 2,00 puntos) Usar el procedimiento “insertar” para insertar en Oracle los ocho
            //emisores que sumen mayor importe, debiendo obtenerlos con una consulta a
            //MongoDB. Cada llamada a “insertar” recibirá un emisor y su importe total. Se valorará
            //que todas estas inserciones se hagan en la misma transacción.
            System.out.println("6");
            /*
          db.compras.aggregate([
          {$group:{_id:"$emisor", importeTotal:{$sum:"$importe"}}},
          {$sort:{"importeTotal":-1}},
          {$limit:8}])
             */
            try {
                Bson bsonGroup = Aggregates.group("$emisor", Accumulators.sum("importeTotal", "$importe"));
                Bson bsonSort = Aggregates.sort(new Document("importeTotal", -1));
                Bson bsonLimit = Aggregates.limit(8);
                List<Document> mejores8 = col.aggregate(List.of(bsonGroup, bsonSort, bsonLimit)).into(new ArrayList<>());
                for (Document doc :
                        mejores8) {
                    PreparedStatement psInsertar = oracle.prepareCall("{call insertar(?,?)}");
                    psInsertar.setString(1, doc.getString("_id"));
                    psInsertar.setDouble(2, doc.getDouble("importeTotal"));
                    psInsertar.execute();
                }
                oracle.commit();
            } catch (Exception e) {
                oracle.rollback();
                System.err.println(e.getMessage());
            }
            //7. (máx. 0,50 puntos) Invocar la función “calcularDescuento” de Oracle pasándole la
            //cantidad de documentos que se insertaron en MongoDB.
            System.out.println("7");

            baseX.execute("CLOSE");

            CallableStatement csCalcularDescuento= oracle.prepareCall("{? = call calculaDescuento(?)}");
            csCalcularDescuento.registerOutParameter(1, Types.DOUBLE);
            csCalcularDescuento.setLong(2, col.countDocuments());
            csCalcularDescuento.execute();
            Double descuento= csCalcularDescuento.getDouble(1);
            System.out.println("Descuento a aplicar: "+descuento);

            //8. (máx. 1,00 punto) Añadir a los documentos de MongoDB cuyo importe sea mayor que
            //500 un campo “descuento” cuyo valor sea el devuelto por la función anterior. Debe
            //visualizarse cuántos documentos cumplieron el criterio de búsqueda, y cuántos se
            //modificaron
            System.out.println("8");
            /*
            db.compras.updateMany({importe:{$gt:500}},{$set:{descuento:"descuentoAAplicar"}})
             */
            Bson bsonImporte= Filters.gt("importe",500);
            Bson bsonDescuento=new Document("$set", new Document("descuento", descuento));
            UpdateResult result = col.updateMany(bsonImporte, bsonDescuento);
            System.out.println("Documentos encontrados: "+result.getMatchedCount()+ "Documentos modificados: "+result.getModifiedCount());
        }
    }
}