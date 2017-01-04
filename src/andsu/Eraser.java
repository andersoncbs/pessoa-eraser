package andsu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Eraser {

    private Connection conn;

    public static void main(String[] args) {
        Eraser eraser = new Eraser();
        eraser.init();
    }

    private void init() {
        apagarEmLote();
    }

    protected void apagarEmLote() {
        Long[] vet = new Long[] {
            7435223115L,
            682705934L,
            4280024120L,
            27568067149L,
            6008895053L,
            55065570787L,
            9596062549L,
            438456149L,
            352365668L,
            15992110763L
        };

        iniciarConexao();
        int tot = 0;
        try {
            conn.setAutoCommit(false);

            for (int i = 0; i < vet.length; i++) {
                try {
                    apagarPessoa(vet[i], conn);
                    conn.commit();
                    tot++;
                } catch (PessoaEraserException e) {}
                conn.close();
                iniciarConexao();
            }

            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(tot + " pessoa(s) excluída(s).");
        System.out.println("Fim.");
    }

    public Connection getConexaoDES() {
        Connection conexao = null;
        try {
            try {
                Class.forName("oracle.jdbc.xa.client.OracleXADataSource");
            } catch (Exception e) {
                e.printStackTrace();
            }
            conexao = DriverManager.getConnection(
                "jdbc:oracle:thin:@//189.9.224.11:1565/Dbd_92116_sigepe_bddes065", "USR_PESSOA",
                "$p30P13w@rE");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conexao;
    }

    public Connection getConexaoTI() {
        Connection conexao = null;
        try {
            try {
                Class.forName("oracle.jdbc.xa.client.OracleXADataSource");
            } catch (Exception e) {
                e.printStackTrace();
            }
            conexao = DriverManager.getConnection(
                "jdbc:oracle:thin:@//189.9.224.11:1566/Dbd_92116_sigepe_bddes066", "USR_PESSOA",
                "$p30P13w@rE");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conexao;
    }

    public Connection getConexaoVAL() {
        Connection conexao = null;
        try {
            try {
                Class.forName("oracle.jdbc.xa.client.OracleXADataSource");
            } catch (Exception e) {
                e.printStackTrace();
            }
            conexao = DriverManager.getConnection(
                "jdbc:oracle:thin:@//189.9.224.11:1564/Dbd_07222_sigepe_bddes064", "USR_PESSOA",
                "$p30P13w@rE");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conexao;
    }

    public void iniciarConexao() {
        conn = getConexaoDES();
    }

    private void apagarPessoa(Long cpf, Connection conn) {
        System.out.println("Iniciando limpeza do CPF: " + cpf);

        // buscar o PESSOA_HIST ID pelo CPF
        List<Long> idsHistorico = buscarIdsPessoaHistorico(cpf);
        // guardar o id da pessoa e os id's das versões

        if (idsHistorico.isEmpty()) {
            throw new PessoaEraserException("Pessoa não cadastrada no BD.");
        }

        Long idPessoa = buscarIdPessoa(idsHistorico);
        if (idPessoa == null) {
            throw new PessoaEraserException("Pessoa não cadastrada no BD.");
        }

        execScript(conn, "DELETE FROM PESSOA.LOG_ALTERACAO_PESSOA WHERE ID_PESSOA = " + idPessoa.toString());

        execScript(conn, 
            "DELETE FROM PESSOA.DOCUMENTO_METADADO WHERE ID_PESSOA_DOCUMENTO IN (SELECT ID_PESSOA_DOCUMENTO FROM PESSOA.PESSOA_DOCUMENTO WHERE ID_PESSOA = " 
                + idPessoa + ")");
        
        execScript(conn, "DELETE FROM PESSOA.PESSOA_DOCUMENTO WHERE ID_PESSOA = " + idPessoa);

        List<Long> idsVersoes = buscarIdVersoes(idsHistorico);

        // excluir dados de PESSOA.PESSOA_CONTROLE_VERSAO_*
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTR_VERSAO_ENDERECO WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTROLE_VERSAO_EMAIL WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTR_VERSAO_NACIONALID WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTR_VERSAO_DADO_BANCA WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTR_VERSAO_TELEFONE WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTROLE_VERSAO_DEFIC WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTROLE_VERSAO_FORMACA WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));
        execScript(conn,
            "DELETE FROM PESSOA.PESSOA_CONTR_VERSAO_FILIACAO WHERE ID_PESSOA_CONTROLE_VERSAO IN "
                + montarClausulaIn(idsVersoes));

        // Excluir os órfãos das tabelas de dados da pessoa
        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_ENDERECO PE "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTR_VERSAO_ENDERECO PCVE WHERE PE.ID_PESSOA_ENDERECO = PCVE.ID_PESSOA_ENDERECO) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_EMAIL PE "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTROLE_VERSAO_EMAIL PCVE WHERE PE.ID_PESSOA_EMAIL = PCVE.ID_PESSOA_EMAIL) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_NACIONALIDADE PN "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTR_VERSAO_NACIONALID PCVN WHERE PN.ID_PESSOA_NACIONALIDADE = PCVN.ID_PESSOA_NACIONALIDADE) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_DADO_BANCARIO PDB "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTR_VERSAO_DADO_BANCA PCVDB WHERE PDB.ID_PESSOA_DADO_BANCARIO = PCVDB.ID_PESSOA_DADO_BANCARIO) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_TELEFONE PT "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTR_VERSAO_TELEFONE PCVT WHERE PT.ID_PESSOA_TELEFONE = PCVT.ID_PESSOA_TELEFONE) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_FORMACAO PF "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTROLE_VERSAO_FORMACA PCVF WHERE PF.ID_PESSOA_FORMACAO = PCVF.ID_PESSOA_FORMACAO) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_FILIACAO PF "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTR_VERSAO_FILIACAO PCVF WHERE PF.ID_PESSOA_FILIACAO = PCVF.ID_PESSOA_FILIACAO) ");

        execScript(
            conn,
            "DELETE FROM PESSOA.PESSOA_DEFICIENCIA PD "
                + "WHERE NOT EXISTS (SELECT 1 FROM PESSOA.PESSOA_CONTROLE_VERSAO_DEFIC PCVD WHERE PD.ID_PESSOA_DEFICIENCIA = PCVD.ID_PESSOA_DEFICIENCIA) ");


        // EXCLUIR
        // PESSOA_CONTROLE_VERSAO
        execScript(conn, "DELETE FROM PESSOA.PESSOA_CONTROLE_VERSAO WHERE ID_PESSOA_CONTROLE_VERSAO IN "
            + montarClausulaIn(idsVersoes));

        // PESSOA_HIST
        execScript(conn, "DELETE FROM PESSOA.PESSOA_HIST WHERE ID_PESSOA_HIST IN "
            + montarClausulaIn(idsHistorico));

        // TIPO_PESSOA
        execScript(conn, "DELETE FROM PESSOA.HIST_TIPO_PESSOA WHERE ID_PESSOA = " + idPessoa.toString());
        
        // PESSOA_INCONSISTENCIA
        execScript(conn, "DELETE FROM PESSOA.PESSOA_INCONSISTENCIA WHERE ID_PESSOA = " + idPessoa.toString());

        // PESSOA
        execScript(conn, "DELETE FROM PESSOA.PESSOA WHERE ID_PESSOA = " + idPessoa.toString());
    }

    private void execScript(Connection conn, String script) {
        try {
            System.out.println(script);
            PreparedStatement statement = conn.prepareStatement(script);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<Long> buscarIdsPessoaHistorico(Long cpf) {
        List<Long> idsHistorico = new ArrayList<Long>();
        try {
            PreparedStatement statement;
            if (cpf != null) {
                statement = conn.prepareStatement("SELECT * FROM pessoa.PESSOA_HIST where CO_CPF = :numCpf");
                statement.setLong(1, cpf);
            } else {
                statement = conn.prepareStatement("SELECT * FROM pessoa.PESSOA_HIST where CO_CPF IS NULL");
            }

            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                idsHistorico.add(resultSet.getLong("ID_PESSOA_HIST"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return idsHistorico;
    }

    private Long buscarIdPessoa(List<Long> idsHistorico) {
        List<Long> idsPessoas = new ArrayList<Long>();
        try {
            PreparedStatement statement = conn
                .prepareStatement("SELECT DISTINCT ID_PESSOA FROM PESSOA.PESSOA_CONTROLE_VERSAO WHERE ID_PESSOA_HIST IN "
                    + montarClausulaIn(idsHistorico));
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                idsPessoas.add(resultSet.getLong("ID_PESSOA"));
            }

            if (!idsPessoas.isEmpty()) {
                return idsPessoas.get(0);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    private List<Long> buscarIdVersoes(List<Long> idsHistorico) {
        List<Long> idsVersoes = new ArrayList<Long>();
        try {
            PreparedStatement statement = conn
                .prepareStatement("SELECT DISTINCT ID_PESSOA_CONTROLE_VERSAO FROM PESSOA.PESSOA_CONTROLE_VERSAO WHERE ID_PESSOA_HIST IN "
                    + montarClausulaIn(idsHistorico));
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                idsVersoes.add(resultSet.getLong("ID_PESSOA_CONTROLE_VERSAO"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return idsVersoes;
    }

    private String montarClausulaIn(List<Long> lista) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        for (Long num : lista) {
            buffer.append(num.toString());
            buffer.append(",");
        }

        String retorno = buffer.toString().substring(0, buffer.toString().length() - 1);
        return retorno + ")";
    }

}