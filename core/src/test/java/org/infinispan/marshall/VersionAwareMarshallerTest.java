/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.marshall;

import org.infinispan.Version;
import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.marshall.jboss.JBossMarshallingTest.CustomReadObjectMethod;
import org.infinispan.marshall.jboss.JBossMarshallingTest.ObjectThatContainsACustomReadObjectMethod;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.data.Person;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransactionFactory;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Immutables;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.TraceInformation;
import org.jgroups.stack.IpAddress;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.*;

@Test(groups = "functional", testName = "marshall.VersionAwareMarshallerTest")
public class VersionAwareMarshallerTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(VersionAwareMarshallerTest.class);
   private final VersionAwareMarshaller marshaller = new VersionAwareMarshaller();

   private GlobalTransactionFactory gtf = new GlobalTransactionFactory();
   private List<SerialObject> serialObjects;
   private static final File SERIAL_PATH = new File("core/src/test/resources/serial");
   private static final String SERIAL_EXT = "bin";
   private static final String SERIAL_BIN_NAME = String.format(
         "serial-%s.%s", Version.MAJOR_MINOR, SERIAL_EXT);
   private static final String SERIAL_DEBUG_NAME = String.format(
         "serial-%s.debug", Version.MAJOR_MINOR);
   private static final Charset UTF_8 = Charset.forName("UTF-8");
   private static final String LS = System.getProperty("line.separator");

   @BeforeClass
   public void initSerialState() {
      // Ordered based on insertion
      serialObjects = new LinkedList<SerialObject>();
      SERIAL_PATH.mkdirs();
   }

   @BeforeTest
   public void setUp() {
      marshaller.inject(Thread.currentThread().getContextClassLoader(), new RemoteCommandsFactory());
      marshaller.start();
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() throws Exception {
      marshaller.stop();
      // After running all tests, persist the serialized data
      writeSerial();
   }

   public void testJGroupsAddressMarshalling(Method m) throws Exception {
      JGroupsAddress address = new JGroupsAddress(new IpAddress(12345));
      marshallAndAssertEquality(address, m);
   }

   @Test(dependsOnMethods = "testJGroupsAddressMarshalling")
   public void testGlobalTransactionMarshalling(Method m) throws Exception {
      JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress(12345));
      GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
      marshallAndAssertEquality(gtx, m);
   }

   @Test(dependsOnMethods = "testGlobalTransactionMarshalling")
   public void testListMarshalling(Method m) throws Exception {
      List l1 = new ArrayList();
      List l2 = new LinkedList();
      for (int i = 0; i < 10; i++) {
         JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress(1000 * i));
         GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
         l1.add(gtx);
         l2.add(gtx);
      }
      marshallAndAssertEquality(l1, m);
      marshallAndAssertEquality(l2, m);
   }

   @Test(dependsOnMethods = "testListMarshalling")
   public void testMapMarshalling(Method m) throws Exception {
      Map m1 = new HashMap();
      Map m2 = new TreeMap();
      Map m3 = new HashMap();
      Map<Integer, GlobalTransaction> m4 = new FastCopyHashMap<Integer, GlobalTransaction>();
      for (int i = 0; i < 10; i++) {
         JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress(1000 * i));
         GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
         m1.put(1000 * i, gtx);
         m2.put(1000 * i, gtx);
         m4.put(1000 * i, gtx);
      }
      Map m5 = Immutables.immutableMapWrap(m3);
      marshallAndAssertEquality(m1, m);
      marshallAndAssertEquality(m2, m);
      byte[] bytes = objectToBytes(m4, m);
      Map<Integer, GlobalTransaction> m4Read = (Map<Integer, GlobalTransaction>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<Integer, GlobalTransaction> entry : m4.entrySet()) {
         assert m4Read.get(entry.getKey()).equals(entry.getValue()) : "Writen[" + entry.getValue() + "] and read[" + m4Read.get(entry.getKey()) + "] objects should be the same";
      }

      marshallAndAssertEquality(m5, m);
   }

   @Test(dependsOnMethods = "testMapMarshalling")
   public void testSetMarshalling(Method m) throws Exception {
      Set s1 = new HashSet();
      Set s2 = new TreeSet();
      for (int i = 0; i < 10; i++) {
         Integer integ = 1000 * i;
         s1.add(integ);
         s2.add(integ);
      }
      marshallAndAssertEquality(s1, m);
      marshallAndAssertEquality(s2, m);
   }

   @Test(dependsOnMethods = "testSetMarshalling")
   public void testMarshalledValueMarshalling(Method m) throws Exception {
      Person p = new Person();
      p.setName("Bob Dylan");
      MarshalledValue mv = new MarshalledValue(p, true, marshaller);
      marshallAndAssertEquality(mv, m);
   }

   @Test(dependsOnMethods = "testMarshalledValueMarshalling")
   public void testMarshalledValueGetMarshalling(Method m) throws Exception {
      Pojo ext = new Pojo();
      MarshalledValue mv = new MarshalledValue(ext, true, marshaller);
      byte[] bytes = objectToBytes(mv, m);
      MarshalledValue rmv = (MarshalledValue) marshaller.objectFromByteBuffer(bytes);
      assert rmv.equals(mv) : "Writen[" + mv + "] and read[" + rmv + "] objects should be the same";
      assert rmv.get() instanceof Pojo;
   }

   @Test(dependsOnMethods = "testMarshalledValueGetMarshalling")
   public void testSingletonListMarshalling(Method m) throws Exception {
      GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(12345)), false);
      List l = Collections.singletonList(gtx);
      marshallAndAssertEquality(l, m);
   }

   @Test(dependsOnMethods = "testSingletonListMarshalling")
   public void testTransactionLogMarshalling(Method m) throws Exception {
      GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(12345)), false);
      PutKeyValueCommand command = new PutKeyValueCommand("k", "v", false, null, 0, 0, Collections.EMPTY_SET);
      TransactionLog.LogEntry entry = new TransactionLog.LogEntry(gtx, command);
      byte[] bytes = objectToBytes(entry, m);
      TransactionLog.LogEntry readObj = (TransactionLog.LogEntry) marshaller.objectFromByteBuffer(bytes);
      assert Arrays.equals(readObj.getModifications(), entry.getModifications()) :
            "Writen[" + entry.getModifications() + "] and read[" + readObj.getModifications() + "] objects should be the same";
      assert readObj.getTransaction().equals(entry.getTransaction()) :
            "Writen[" + entry.getModifications() + "] and read[" + readObj.getModifications() + "] objects should be the same";
   }

   @Test(dependsOnMethods = "testTransactionLogMarshalling")
   public void testImmutableResponseMarshalling(Method m) throws Exception {
      marshallAndAssertEquality(RequestIgnoredResponse.INSTANCE, m);
      marshallAndAssertEquality(UnsuccessfulResponse.INSTANCE, m);
   }

   @Test(dependsOnMethods = "testImmutableResponseMarshalling")
   public void testExtendedResponseMarshalling(Method m) throws Exception {
      SuccessfulResponse sr = new SuccessfulResponse("Blah");
      ExtendedResponse extended = new ExtendedResponse(sr, false);
      byte[] bytes = objectToBytes(extended, m);
      ExtendedResponse readObj = (ExtendedResponse) marshaller.objectFromByteBuffer(bytes);
      assert extended.getResponse().equals(readObj.getResponse()) :
            "Writen[" + extended.getResponse() + "] and read[" + readObj.getResponse() + "] objects should be the same";
      assert extended.isReplayIgnoredRequests() == readObj.isReplayIgnoredRequests() :
            "Writen[" + extended.isReplayIgnoredRequests() + "] and read[" + readObj.isReplayIgnoredRequests() + "] objects should be the same";
   }

   @Test(dependsOnMethods = "testExtendedResponseMarshalling")
   public void testReplicableCommandsMarshalling(Method m) throws Exception {
      StateTransferControlCommand c1 = new StateTransferControlCommand(true);
      byte[] bytes = objectToBytes(c1, m);
      StateTransferControlCommand rc1 = (StateTransferControlCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc1.getCommandId() == c1.getCommandId() : "Writen[" + c1.getCommandId() + "] and read[" + rc1.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc1.getParameters(), c1.getParameters()) : "Writen[" + c1.getParameters() + "] and read[" + rc1.getParameters() + "] objects should be the same";

      ClusteredGetCommand c2 = new ClusteredGetCommand("key", "mycache", Collections.EMPTY_SET);
      marshallAndAssertEquality(c2, m);

      // SizeCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      GetKeyValueCommand c4 = new GetKeyValueCommand("key", null, Collections.EMPTY_SET);
      bytes = objectToBytes(c4, m);
      GetKeyValueCommand rc4 = (GetKeyValueCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc4.getCommandId() == c4.getCommandId() : "Writen[" + c4.getCommandId() + "] and read[" + rc4.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc4.getParameters(), c4.getParameters()) : "Writen[" + c4.getParameters() + "] and read[" + rc4.getParameters() + "] objects should be the same";

      PutKeyValueCommand c5 = new PutKeyValueCommand("k", "v", false, null, 0, 0, Collections.EMPTY_SET);
      marshallAndAssertEquality(c5, m);

      RemoveCommand c6 = new RemoveCommand("key", null, null, Collections.EMPTY_SET);
      marshallAndAssertEquality(c6, m);

      // EvictCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      InvalidateCommand c7 = new InvalidateCommand(null, null, "key1", "key2");
      bytes = objectToBytes(c7, m);
      InvalidateCommand rc7 = (InvalidateCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc7.getCommandId() == c7.getCommandId() : "Writen[" + c7.getCommandId() + "] and read[" + rc7.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc7.getParameters(), c7.getParameters()) : "Writen[" + c7.getParameters() + "] and read[" + rc7.getParameters() + "] objects should be the same";

      InvalidateCommand c71 = new InvalidateL1Command(false, null, null, null, null, "key1", "key2");
      bytes = objectToBytes(c71, m);
      InvalidateCommand rc71 = (InvalidateCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc71.getCommandId() == c71.getCommandId() : "Writen[" + c71.getCommandId() + "] and read[" + rc71.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc71.getParameters(), c71.getParameters()) : "Writen[" + c71.getParameters() + "] and read[" + rc71.getParameters() + "] objects should be the same";

      ReplaceCommand c8 = new ReplaceCommand("key", "oldvalue", "newvalue", 0, 0, Collections.EMPTY_SET);
      marshallAndAssertEquality(c8, m);

      ClearCommand c9 = new ClearCommand();
      bytes = objectToBytes(c9, m);
      ClearCommand rc9 = (ClearCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc9.getCommandId() == c9.getCommandId() : "Writen[" + c9.getCommandId() + "] and read[" + rc9.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc9.getParameters(), c9.getParameters()) : "Writen[" + c9.getParameters() + "] and read[" + rc9.getParameters() + "] objects should be the same";

      Map m1 = new HashMap();
      for (int i = 0; i < 10; i++) {
         GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(1000 * i)), false);
         m1.put(1000 * i, gtx);
      }

      PutMapCommand c10 = new PutMapCommand(m1, null, 0, 0, Collections.EMPTY_SET);
      marshallAndAssertEquality(c10, m);

      Address local = new JGroupsAddress(new IpAddress(12345));
      GlobalTransaction gtx = gtf.newGlobalTransaction(local, false);
      PrepareCommand c11 = new PrepareCommand(gtx, true, c5, c6, c8, c10);
      marshallAndAssertEquality(c11, m);

      CommitCommand c12 = new CommitCommand(gtx);
      marshallAndAssertEquality(c12, m);

      RollbackCommand c13 = new RollbackCommand(gtx);
      marshallAndAssertEquality(c13, m);

      MultipleRpcCommand c99 = new MultipleRpcCommand(Arrays.asList(c2, c5, c6, c8, c10, c12, c13), "mycache");
      marshallAndAssertEquality(c99, m);
   }

   @Test(dependsOnMethods = "testReplicableCommandsMarshalling")
   public void testInternalCacheEntryMarshalling(Method m) throws Exception {
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      marshallAndAssertEquality(entry1, m);

      MortalCacheEntry entry2 = (MortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      marshallAndAssertEquality(entry2, m);

      TransientCacheEntry entry3 = (TransientCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      marshallAndAssertEquality(entry3, m);

      TransientMortalCacheEntry entry4 = (TransientMortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      marshallAndAssertEquality(entry4, m);
   }

   @Test(dependsOnMethods = "testInternalCacheEntryMarshalling")
   public void testInternalCacheValueMarshalling(Method m) throws Exception {
      ImmortalCacheValue value1 = (ImmortalCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      byte[] bytes = objectToBytes(value1, m);
      ImmortalCacheValue rvalue1 = (ImmortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue1.getValue().equals(value1.getValue()) : "Writen[" + rvalue1.getValue() + "] and read[" + value1.getValue() + "] objects should be the same";

      MortalCacheValue value2 = (MortalCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      bytes = objectToBytes(value2, m);
      MortalCacheValue rvalue2 = (MortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue2.getValue().equals(value2.getValue()) : "Writen[" + rvalue2.getValue() + "] and read[" + value2.getValue() + "] objects should be the same";

      TransientCacheValue value3 = (TransientCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      bytes = objectToBytes(value3, m);
      TransientCacheValue rvalue3 = (TransientCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue3.getValue().equals(value3.getValue()) : "Writen[" + rvalue3.getValue() + "] and read[" + value3.getValue() + "] objects should be the same";

      TransientMortalCacheValue value4 = (TransientMortalCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      bytes = objectToBytes(value4, m);
      TransientMortalCacheValue rvalue4 = (TransientMortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue4.getValue().equals(value4.getValue()) : "Writen[" + rvalue4.getValue() + "] and read[" + value4.getValue() + "] objects should be the same";
   }

   @Test(dependsOnMethods = "testInternalCacheValueMarshalling")
   public void testBucketMarshalling(Method m) throws Exception {
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      MortalCacheEntry entry2 = (MortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      TransientCacheEntry entry3 = (TransientCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      TransientMortalCacheEntry entry4 = (TransientMortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      Bucket b = new Bucket();
      b.setBucketName("mybucket");
      b.addEntry(entry1);
      b.addEntry(entry2);
      b.addEntry(entry3);
      b.addEntry(entry4);

      byte[] bytes = objectToBytes(b, m);
      Bucket rb = (Bucket) marshaller.objectFromByteBuffer(bytes);
      assert rb.getEntries().equals(b.getEntries()) : "Writen[" + b.getEntries() + "] and read[" + rb.getEntries() + "] objects should be the same";
   }

   @Test(dependsOnMethods = "testBucketMarshalling")
   public void testLongPutKeyValueCommand(Method m) throws Exception {
      PutKeyValueCommand c = new PutKeyValueCommand("SESSION_173", "@TSXMHVROYNOFCJVEUJQGBCENNQDEWSCYSOHECJOHEICBEIGJVTIBB@TVNCWLTQCGTEJ@NBJLTMVGXCHXTSVE@BCRYGWPRVLXOJXBRJDVNBVXPRTRLBMHPOUYQKDEPDSADUAWPFSIOCINPSSFGABDUXRMTMMJMRTGBGBOAMGVMTKUDUAJGCAHCYW@LAXMDSFYOSXJXLUAJGQKPTHUKDOXRWKEFIVRTH@VIMQBGYPKWMS@HPOESTPIJE@OTOTWUWIOBLYKQQPTNGWVLRRCWHNIMWDQNOO@JHHEVYVQEODMWKFKKKSWURVDLXPTFQYIHLIM@GSBFWMDQGDQIJONNEVHGQTLDBRBML@BEWGHOQHHEBRFUQSLB@@CILXEAVQQBTXSITMBXHMHORHLTJF@MKMHQGHTSENWILTAKCCPVSQIPBVRAFSSEXIOVCPDXHUBIBUPBSCGPRECXEPMQHRHDOHIHVBPNDKOVLPCLKAJMNOTSF@SRXYVUEMQRCXVIETXVHOVNGYERBNM@RIMGHC@FNTUXSJSKALGHAFHGTFEANQUMBPUYFDSGLUYRRFDJHCW@JBWOBGMGTITAICRC@TPVCRKRMFPUSRRAHI@XOYKVGPHEBQD@@APEKSBCTBKREWAQGKHTJ@IHJD@YFSRDQPA@HKKELIJGFDYFEXFCOTCQIHKCQBLVDFHMGOWIDOWMVBDSJQOFGOIAPURRHVBGEJWYBUGGVHE@PU@NMQFMYTNYJDWPIADNVNCNYCCCPGODLAO@YYLVITEMNNKIFSDXKORJYWMFGKNYFPUQIC@AIDR@IWXCVALQBDOXRWIBXLKYTWDNHHSCUROAU@HVNENDAOP@RPTRIGLLLUNDQIDXJDDNF@P@PA@FEIBQKSKFQITTHDYGQRJMWPRLQC@NJVNVSKGOGYXPYSQHKPALKLFWNAOSQFTLEPVOII@RPDNRCVRDUMMFIVSWGIASUBMTGQSDGB@TBBYECFBRBGILJFCJ@JIQIQRVJXWIPGNVXKYATSPJTIPGCMCNPOKNEHBNUIAEQFQTYVLGAR@RVWVA@RMPBX@LRLJUEBUWO@PKXNIP@FKIQSVWKNO@FOJWDSIOLXHXJFBQPPVKKP@YKXPOOMBTLXMEHPRLLSFSVGMPXXNBCYVVSPNGMFBJUDCVOVGXPKVNTOFKVJUJOSDHSCOQRXOKBVP@WCUUFGMJAUQ@GRAGXICFCFICBSNASUBPAFRIPUK@OXOCCNOGTTSFVQKBQNB@DWGVEFSGTAXAPLBJ@SYHUNXWXPMR@KPFAJCIXPDURELFYPMUSLTJSQNDHHKJTIWCGNEKJF@CUWYTWLPNHYPHXNOGLSICKEFDULIXXSIGFMCQGURSRTUJDKRXBUUXIDFECMPXQX@CVYLDABEMFKUGBTBNMNBPCKCHWRJKSOGJFXMFYLLPUVUHBCNULEFAXPVKVKQKYCEFRUYPBRBDBDOVYLIQMQBLTUK@PRDCYBOKJGVUADFJFAFFXKJTNAJTHISWOSMVAYLIOGIORQQWFAKNU@KHPM@BYKTFSLSRHBATQTKUWSFAQS@Y@QIKCUWQYTODBRCYYYIAFMDVRURKVYJXHNGVLSQQFCXKLNUPCTEJSWIJUBFELSBUHANELHSIWLVQSSAIJRUEDOHHX@CKEBPOJRLRHEPLENSCDGEWXRTVUCSPFSAJUXDJOIUWFGPKHBVRVDMUUCPUDKRKVAXPSOBOPKPRRLFCKTLH@VGWKERASJYU@JAVWNBJGQOVF@QPSGJVEPAV@NAD@@FQRYPQIOAURILWXCKINPMBNUHPUID@YDQBHWAVDPPWRFKKGWJQTI@@OPSQ@ROUGHFNHCJBDFCHRLRTEMTUBWVCNOPYXKSSQDCXTOLOIIOCXBTPAUYDICFIXPJRB@CHFNXUCXANXYKXAISDSSLJGQOLBYXWHG@@KPARPCKOXAYVPDGRW@LDCRQBNMJREHWDYMXHEXAJQKHBIRAVHJQIVGOIXNINYQMJBXKM@DXESMBHLKHVSFDLVPOSOVMLHPSHQYY@DNMCGGGAJMHPVDLBGJP@EVDGLYBMD@NWHEYTBPIBPUPYOPOJVV@IVJXJMHIWWSIRKUWSR@U@@TDVMG@GRXVLCNEIISEVIVPOMJHKOWMRMITYDUQASWJIKVNYUFQVDT@BHTOMFXVFRKAARLNOGX@ADWCKHOVEMIGBWXINCUXEMVHSJJQDU@INTHDJQPSAQNAYONDBBFYGBTNGUSJHRKLCPHQMNLDHUQJPLLCDVTYLXTHJCBUXCRDY@YI@IQDCLJBBJC@NXGANXFIWPPNFVTDJWQ@@BIYJONOFP@RHTQEYPVHPPUS@UUENSNNF@WVGTSAVKDSQNMHP@VJORGTVWXVBPWKQNRWLSQFSBMXQKWRYMXPAYREXYGONKEWJMBCSLB@KSHXMIWMSBDGQWPDMUGVNMEWKMJKQECIRRVXBPBLGAFTUFHYSHLF@TGYETMDXRFAXVEUBSTGLSMWJMXJWMDPPDAFGNBMTQEMBDLRASMUMU@QTCDCPEGODHESDQVEIQYBJJPFXDLWPUNFAREYCY@YDDSTMKWCANNPXF@@WLMEXRPUNTWNOX@YKFNNTGMXIBBDA@TYLPJFNFHPQKMSNCLBME@FBPOIYNSDFBLHITKIFEFNXXOJAAFMRTGPALOANXF@YPY@RYTVOW@AKNM@C@LJKGBJMUYGGTXRHQCPOLNOGPPS@YSKAJSTQHLRBXUACXJYBLJSEHDNMLLUBSOIHQUI@VUNF@XAVRXUCYNCBDDGUDNVRYP@TPFPKGVNPTEDOTTUUFKCHQ@WWASQXLCBHNRBVSD@NVYT@GJQYSQGYPJO@WSEYDVKCBWANAFUWLDXOQYCYP@BSJFCBTXGKUNWLWUCYL@TNOWGDFHQTWQVYLQBBRQVMGNDBVXEFXTMMVYSHNVTTQAJCHKULOAJUSGJRPHQFCROWE@OMFUVRKGCWED@IAQGRLADOJGQKLCL@FCKTSITGMJRCCMPLOS@ONPQWFUROXYAUJQXIYVDCYBPYHPYCXNCRKRKLATLWWXLBLNOPUJFUJEDOIRKS@MMYPXIJNXPFOQJCHSCBEBGDUQYXQAWEEJDOSINXYLDXUJCQECU@WQSACTDFLGELHPGDFVDXFSSFOSYDLHQFVJESNAVAHKTUPBTPLSFSHYKLEXJXGWESVQQUTUPU@QXRTIDQ@IXBBOYINNHPEMTPRVRNJPQJFACFXUBKXOFHQSPOTLCQ@PLWGEFNKYCYFMKWPFUP@GLHKNMASGIENCACUISTG@YNQCNSOSBKOIXORKSHEOXHSMJJRUICJTCK@PWFRBPLXU@MUEMPFGDLUJEKD@ROUFBLKATXUCHEAQHEYDLCFDIRJSAXTV@CYMPQNMLTMFAHPRBLNSCVFBJMKQLAHWYIOLRMTOY@@RNKTUXHFYUMHGKCCGNEOIOQCISJEHCEVTTWM@TLFRIFDREHFBTTDEJRUNTWAEETGSVDOR@@UQNKFERMBVFJBOAYHPOKMSMRIERDA@JXYSJ@ORER@MBAVWCVGFNA@FRRPQSIIOIUGAJKVQXGINUUKPJPLQRMHPUBETEEIMIBPM@PETR@XD@DOHGRIBVXKLXQWHUFMTWEDYWFWRLPGDS@TANUXGIDTRVXKVCVEXYRKXQCTI@WNSFRAHJJGG@NIPPAAOJXQRTCLBYKDA@FFGHNUIGBFKOQMEDUEFELFLNKPCHA@OXJJRYNPDFSXIFSJYTDMSSBHDPUSQQDAVD@JAAWJDSVTERAJBFEPVRWKMYAPISPWLDPSRE@UMRQLXERTWRDLQVMVCOM@NYPXFLWMWKALMQVNJ@HCTMMIOLRWBJHCYFLMM@IWXPSHRRUNICSSWHOQHUVJE@HKJAADLBTPVLDAKCHRSURJCAXYTMYKHQMWDAWWASUW@HWGBVPTRHJGDWOGHPCNWSXTNKWONQGEKDDWGCKWVSAD@YLCCENMCHALHVDYQW@NQGNCY@M@GGV@RIR@OUS@PQIJMCFEIMGPYBXYR@NSIAUEXT@MOCNWRMLYHUUAFJCCLLRNFGKLPPIIH@BYRME@UJAKIFHOV@ILP@BGXRNJBIBARSOIMTDSHMGPIGRJBGHYRYXPFUHVOOMCQFNLM@CNCBTGO@UKXBOICNVCRGHADYQVAMNSFRONJ@WITET@BSHMQLWYMVGMQJVSJOXOUJDSXYVVBQJSVGREQLIQKWC@BMDNONHXFYPQENSJINQYKHVCTUTG@QQYJKJURDCKJTUQAM@DWNXWRNILYVAAJ@IADBIXKEIHVXLXUVMGQPAQTWJCDMVDVYUDTXQTCYXDPHKBAGMTAMKEM@QNOQJBREXNWFCXNXRPGOGEIR@KQJIGXAWXLTNCX@ID@XNRNYGRF@QPNWEX@XH@XKSXLQTLQPFSHAHXJLHUTNQWFFAJYHBWIFVJELDPSPLRRDPPNXSBYBEREEELIWNVYXOXYJQAIGHALUAWNUSSNMBHBFLRMMTKEKNSINECUGWTDNMROXI@BJJXKSPIIIXOAJBFVSITQDXTODBGKEPJMWK@JOL@SWTCGSHCOPHECTPJFUXIHUOSVMUTNNSLLJDEOMAGIXEAAVILRMOJXVHHPNPUYYODMXYAYGHI@BUB@NLP@KNPCYFRWAFES@WISBACDSPELEVTJEBNRVENSXXEVDVC@RIDIDSBPQIQNNSRPS@HCJ@XPIOFDXHUBCNFQKHMUYLXW@LMFMALHLESSXCOULRWDTJIVKKTLGFE@HKGVKUGMVHWACQOTSVNWBNUUGTMSQEJ@DXJQQYPOWVRQNQKXSLOEAA@@FRDCGCCQWQ@IY@EATGQGQIETPIJHOIQRYWLTGUENQYDNQSBI@IAUDEWDKICHNUGNAIXNICMBK@CJGSASMTFKWOBSI@KULNENWXV@VNFOANM@OJHFVV@IYRMDB@LHSGXIJMMFCGJKTKDXSMY@FHDNY@VSDUORGWVFMVKJXOCCDLSLMHCSXFBTW@RQTFNRDJUIKRD@PWPY", false, null, 0, 0, Collections.EMPTY_SET);
      marshallAndAssertEquality(c, m);
   }

   @Test(dependsOnMethods = "testLongPutKeyValueCommand")
   public void testExceptionResponse(Method m) throws Exception {
      ExceptionResponse er = new ExceptionResponse(new TimeoutException());
      byte[] bytes = objectToBytes(er, m);
      ExceptionResponse rer = (ExceptionResponse) marshaller.objectFromByteBuffer(bytes);
      assert rer.getException().getClass().equals(er.getException().getClass()) : "Writen[" + er.getException().getClass() + "] and read[" + rer.getException().getClass() + "] objects should be the same";
   }

   @Test(dependsOnMethods = "testExceptionResponse")
   public void testAtomicHashMap(Method m) throws Exception {
      AtomicHashMap<String, String> atomicHashMap = new AtomicHashMap<String, String>();
      atomicHashMap.initForWriting();
      atomicHashMap.put("k1", "v1");
      atomicHashMap.put("k1", "v2");
      atomicHashMap.put("k1", "v3");
      assert atomicHashMap.size() == 1;
      byte[] bytes = objectToBytes(atomicHashMap, m);
      atomicHashMap = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<String, String> entry : atomicHashMap.entrySet()) {
         assert atomicHashMap.get(entry.getKey()).equals(entry.getValue());
      }
      assert atomicHashMap.size() == 1;

      atomicHashMap = new AtomicHashMap<String, String>();
      assert atomicHashMap.isEmpty();
      bytes = objectToBytes(atomicHashMap, m);
      atomicHashMap = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      assert atomicHashMap.isEmpty();

      atomicHashMap = new AtomicHashMap<String, String>();
      atomicHashMap.initForWriting();
      atomicHashMap.put("k1", "v1");
      atomicHashMap.put("k2", "v2");
      atomicHashMap.put("k3", "v3");
      atomicHashMap.remove("k1");
      assert atomicHashMap.size() == 2;
      bytes = objectToBytes(atomicHashMap, m);
      atomicHashMap = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<String, String> entry : atomicHashMap.entrySet()) {
         assert atomicHashMap.get(entry.getKey()).equals(entry.getValue());
      }
      assert atomicHashMap.size() == 2;
      
      atomicHashMap = new AtomicHashMap<String, String>();
      atomicHashMap.initForWriting();
      atomicHashMap.put("k5", "v1");
      atomicHashMap.put("k5", "v2");
      atomicHashMap.put("k5", "v3");
      atomicHashMap.clear();
      assert atomicHashMap.isEmpty();
      bytes = objectToBytes(atomicHashMap, m);
      atomicHashMap = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<String, String> entry : atomicHashMap.entrySet()) {
         assert atomicHashMap.get(entry.getKey()).equals(entry.getValue());
      }
      assert atomicHashMap.isEmpty();
   }

   @Test(dependsOnMethods = "testAtomicHashMap")
   public void testMarshallObjectThatContainsACustomReadObjectMethod(Method m) throws Exception {
      ObjectThatContainsACustomReadObjectMethod obj = new ObjectThatContainsACustomReadObjectMethod();
      obj.anObjectWithCustomReadObjectMethod = new CustomReadObjectMethod();
      marshallAndAssertEquality(obj, m);
   }

   @Test(dependsOnMethods = "testMarshallObjectThatContainsACustomReadObjectMethod")
   public void testMIMECacheEntryMarshalling(Method m) throws Exception {
      MIMECacheEntry entry = new MIMECacheEntry("rm", new byte[] {1, 2, 3});
      byte[] bytes = objectToBytes(entry, m);
      MIMECacheEntry rEntry = (MIMECacheEntry) marshaller.objectFromByteBuffer(bytes);
      assert Arrays.equals(rEntry.data, entry.data);
      assert rEntry.contentType.equals(entry.contentType);
      assert rEntry.lastModified == entry.lastModified;
   }

   @Test(dependsOnMethods = "testMIMECacheEntryMarshalling")
   public void testNestedNonSerializable(Method m) throws Exception {
      PutKeyValueCommand cmd = new PutKeyValueCommand("k", new Object(), false, null, 0, 0, Collections.EMPTY_SET);
      try {
         objectToBytes(cmd, m);
      } catch (NotSerializableException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         assert inf.toString().contains("in object java.lang.Object@");
         assert inf.toString().contains("in object org.infinispan.commands.write.PutKeyValueCommand@");
      }
   }

   @Test(dependsOnMethods = "testNestedNonSerializable")
   public void testNonSerializable(Method m) throws Exception {
      try {
         objectToBytes(new Object(), m);
      } catch (NotSerializableException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         assert inf.toString().contains("in object java.lang.Object@");
      }
   }

   static class PojoWhichFailsOnUnmarshalling extends Pojo {
      private static final long serialVersionUID = -5109779096242560884L;

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         throw new IOException("Injected failue!");
      }
      
   };
   
   @Test(dependsOnMethods = "testNonSerializable")
   public void testErrorUnmarshalling(Method m) throws Exception {
      Pojo pojo = new PojoWhichFailsOnUnmarshalling();
      // Do not cache the serialized version of this object cos it'll lead to failure
      byte[] bytes = marshaller.objectToByteBuffer(pojo);
      try {
         marshaller.objectFromByteBuffer(bytes);
      } catch (IOException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         assert inf.toString().contains("in object of type org.infinispan.marshall.VersionAwareMarshallerTest$PojoWhichFailsOnUnmarshalling");
      }
      
   }

   @Test(dependsOnMethods = "testErrorUnmarshalling")
   public void testMarshallingSerializableSubclass(Method m) throws Exception {
      Child1 child1Obj = new Child1(1234, "1234");
      byte[] bytes = objectToBytes(child1Obj, m);
      marshaller.objectFromByteBuffer(bytes);
   }

   @Test(dependsOnMethods = "testMarshallingSerializableSubclass")
   public void testMarshallingNestedSerializableSubclass(Method m) throws Exception {
      Child1 child1Obj = new Child1(1234, "1234");
      Child2 child2Obj = new Child2(2345, "2345", child1Obj);
      byte[] bytes = objectToBytes(child2Obj, m);
      marshaller.objectFromByteBuffer(bytes);
   }

   @Test(dependsOnMethods = "testMarshallingNestedSerializableSubclass")
   public void testByteArrayKey(Method m) throws Exception {
      ByteArrayKey o = new ByteArrayKey("123".getBytes());
      marshallAndAssertEquality(o, m);
   }

   // IMPORTANT! Insert new tests here and adjust test method dependency!

   @Test(dependsOnMethods = "testByteArrayKey")
   public void testUnmarshallingPersistedSerialData() throws Exception {
      String[] dataFileNames = SERIAL_PATH.list(new FilenameFilter() {
         @Override
         public boolean accept(File dir, String name) {
            return name.endsWith(SERIAL_EXT);
         }
      });
      for (String dataFileName : dataFileNames) {
         FileInputStream fileIS = new FileInputStream(new File(SERIAL_PATH, dataFileName));
         BufferedInputStream bufferedIS = new BufferedInputStream(fileIS);
         ObjectInputStream objectIS = new ObjectInputStream(bufferedIS);
         int numObjs = objectIS.readInt();
         for (int i = 0; i < numObjs; i++) {
            try {
               marshaller.objectFromByteBuffer((byte[]) objectIS.readObject());
               // TODO: This could be enhanced to also verify the contents:
               // Doing this would require some thinking to be able to either:
               // a) Make sure expected values do not change over diff versions
               //    of this class, and call back to the methods that assert the
               //    equality object.
               // b) Or store the expected values along side the serialized version.
            } catch (Throwable t) {
               throw new Exception("Unable to read marshalled object in " + dataFileName, t);
            }
         }
      }
   }

   protected void marshallAndAssertEquality(Object writeObj, Method m) throws Exception {
      byte[] bytes = objectToBytes(writeObj, m);
      Object readObj = marshaller.objectFromByteBuffer(bytes);
      assert readObj.equals(writeObj) : "Writen[" + writeObj + "] and read[" + readObj + "] objects should be the same";
   }

   private byte[] objectToBytes(Object writeObj, Method m) throws IOException, InterruptedException {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      // Cache payload for later storing in serialization data file
      serialObjects.add(new SerialObject()
            .object(writeObj).bytes(bytes)
            .hexBinary(Util.printArray(bytes, false, true))
            .testMethod(m.getName()));
      return bytes;
   }

   private void writeSerial() throws Exception {
      writeSerialBin();
      writeSerialDebug();
   }

   private void writeSerialBin() throws IOException {
      FileOutputStream fileOS = new FileOutputStream(
            new File(SERIAL_PATH, SERIAL_BIN_NAME));
      BufferedOutputStream bufferedOS = new BufferedOutputStream(fileOS);
      ObjectOutputStream objectOS = new ObjectOutputStream(bufferedOS);
      try {
         objectOS.writeInt(serialObjects.size());
         for (SerialObject serialObject : serialObjects)
            objectOS.writeObject(serialObject.bytes);

         objectOS.flush();
      } finally {
         objectOS.close();
         try {
            bufferedOS.flush();
         } finally {
            bufferedOS.close();
            try {
               fileOS.flush();
            } finally {
               fileOS.close();
            }
         }
      }
   }

   private void writeSerialDebug() throws IOException {
      Writer out = new OutputStreamWriter(new FileOutputStream(
            new File(SERIAL_PATH, SERIAL_DEBUG_NAME)), UTF_8.name());

      try {
         for (SerialObject serialObject : serialObjects) {
            out.write("testMethod=" + serialObject.testMethod);
            out.write(LS);
            out.write("type=" + serialObject.object.getClass().getName());
            out.write(LS);
            out.write("toString=" + serialObject.object.toString());
            out.write(LS);
            out.write("hexBinary=" + serialObject.hexBinary);
            out.write(LS);
            out.write(LS);
         }
      } finally {
         out.close();
      }
   }

   static class SerialObject {

      Object object;
      byte[] bytes;
      String hexBinary;
      String testMethod;

      SerialObject object(Object object) {
         this.object = object;
         return this;
      }

      SerialObject bytes(byte[] bytes) {
         this.bytes = bytes;
         return this;
      }

      SerialObject hexBinary(String hexBinary) {
         this.hexBinary = hexBinary;
         return this;
      }

      SerialObject testMethod(String testMethod) {
         this.testMethod = testMethod;
         return this;
      }

   }

   static class Pojo implements Externalizable {
      int i;
      boolean b;
      static int serializationCount, deserializationCount;
      private static final long serialVersionUID = 9032309454840083326L;

      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (b != pojo.b) return false;
         if (i != pojo.i) return false;

         return true;
      }

      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         serializationCount++;
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         deserializationCount++;
      }
   }

   static class Parent implements Serializable {
       private String id;
       private Child1 child1Obj;

       public Parent(String id, Child1 child1Obj) {
           this.id = id;
           this.child1Obj = child1Obj;
       }

       public String getId() {
           return id;
       }
       public Child1 getChild1Obj() {
           return child1Obj;
       }
   }

   static class Child1 extends Parent {
       private int someInt;

       public Child1(int someInt, String parentStr) {
           super(parentStr, null);
           this.someInt = someInt;
       }

   }

   static class Child2 extends Parent {
       private int someInt;

       public Child2(int someInt, String parentStr, Child1 child1Obj) {
           super(parentStr, child1Obj);
           this.someInt = someInt;
       }
   }
}
