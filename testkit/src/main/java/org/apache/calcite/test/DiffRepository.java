/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.linq4j.Nullness;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.XmlOutput;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static java.util.Objects.requireNonNull;

/**
 * A collection of resources used by tests.
 *
 * <p>Loads files containing test input and output into memory. If there are
 * differences, writes out a log file containing the actual output.
 *
 * <p>Typical usage is as follows. A test case class defines a method
 *
 * <blockquote><pre><code>
 * package com.acme.test;
 * &nbsp;
 * public class MyTest extends TestCase {
 *   public DiffRepository getDiffRepos() {
 *     return DiffRepository.lookup(MyTest.class);
 *   }
 * &nbsp;
 *   &#64;Test void testToUpper() {
 *     getDiffRepos().assertEquals("${result}", "${string}");
 *   }
 * &nbsp;
 *   &#64;Test void testToLower() {
 *     getDiffRepos().assertEquals("Multi-line\nstring", "${string}");
 *   }
 * }
 * </code></pre></blockquote>
 *
 * <p>There is an accompanying reference file named after the class,
 * <code>src/test/resources/com/acme/test/MyTest.xml</code>:
 *
 * <blockquote><pre><code>
 * &lt;Root&gt;
 *     &lt;TestCase name="testToUpper"&gt;
 *         &lt;Resource name="string"&gt;
 *             &lt;![CDATA[String to be converted to upper case]]&gt;
 *         &lt;/Resource&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[STRING TO BE CONVERTED TO UPPER CASE]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 *     &lt;TestCase name="testToLower"&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[multi-line
 * string]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 * &lt;/Root&gt;
 *
 * </code></pre></blockquote>
 *
 * <p>If any of the test cases fails, a log file is generated, called
 * {@code build/diffrepo/test/com/acme/test/MyTest_actual.xml},
 * containing the actual output.
 *
 * <p>The log
 * file is otherwise identical to the reference log, so once the log file has
 * been verified, it can simply be copied over to become the new reference
 * log:
 *
 * <blockquote>{@code
 * cp build/diffrepo/test/com/acme/test/MyTest_actual.xml
 * src/test/resources/com/acme/test/MyTest.xml
 * }</blockquote>
 *
 * <p>If a resource or test case does not exist, <code>DiffRepository</code>
 * creates them in the log file. Because DiffRepository is so forgiving, it is
 * very easy to create new tests and test cases.
 *
 * <p>The {@link #lookup} method ensures that all test cases share the same
 * instance of the repository. This is important more than one test case fails.
 * The shared instance ensures that the generated
 * {@code build/diffrepo/test/com/acme/test/MyTest_actual.xml}
 * file contains the actual for <em>both</em> test cases.
 */
public class DiffRepository {
  //~ Static fields/initializers ---------------------------------------------

/*
      Example XML document:

      <Root>
        <TestCase name="testFoo">
          <Resource name="sql">
            <![CDATA[select from emps]]>
           </Resource>
           <Resource name="plan">
             <![CDATA[MockTableImplRel.FENNEL_EXEC(table=[SALES, EMP])]]>
           </Resource>
         </TestCase>
         <TestCase name="testBar">
           <Resource name="sql">
             <![CDATA[select * from depts where deptno = 10]]>
           </Resource>
           <Resource name="output">
             <![CDATA[10, 'Sales']]>
           </Resource>
         </TestCase>
       </Root>
*/
  private static final String ROOT_TAG = "Root";
  private static final String TEST_CASE_TAG = "TestCase";
  private static final String TEST_CASE_NAME_ATTR = "name";
  private static final String TEST_CASE_OVERRIDES_ATTR = "overrides";
  private static final String RESOURCE_TAG = "Resource";
  private static final String RESOURCE_NAME_ATTR = "name";

  /**
   * Holds one diff-repository per class. It is necessary for all test cases in
   * the same class to share the same diff-repository: if the repository gets
   * loaded once per test case, then only one diff is recorded.
   */
  private static final LoadingCache<Key, DiffRepository> REPOSITORY_CACHE =
      CacheBuilder.newBuilder().build(CacheLoader.from(Key::toRepo));

  private static final ThreadLocal<DocumentBuilderFactory> DOCUMENT_BUILDER_FACTORY =
      ThreadLocal.withInitial(() -> {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setXIncludeAware(false);
        documentBuilderFactory.setExpandEntityReferences(false);
        documentBuilderFactory.setNamespaceAware(true);
        try {
          documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
          documentBuilderFactory
              .setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (final ParserConfigurationException e) {
          throw new IllegalStateException("Document Builder configuration failed", e);
        }
        return documentBuilderFactory;
      });

  //~ Instance fields --------------------------------------------------------

  private final @Nullable DiffRepository baseRepository;
  private final int indent;
  private final ImmutableSortedSet<String> outOfOrderTests;
  private final Boolean existsMethodOnlyInXml;
  private final SortedMap<String, Node> xmlTestCases;
  private Document doc;
  private final Element root;
  private final URL refFile;
  private final File logFile;
  private final @Nullable Filter filter;
  private int modCount;
  private int modCountAtLastWrite;

  /**
   * Creates a DiffRepository.
   *
   * @param refFile   Reference file
   * @param logFile   Log file
   * @param baseRepository Parent repository or null
   * @param filter    Filter or null
   * @param indent    Indentation of XML file
   */
  private DiffRepository(URL refFile, File logFile,
      @Nullable DiffRepository baseRepository, @Nullable Filter filter,
      int indent, Set<String> javaTestMethods) {
    this.baseRepository = baseRepository;
    this.filter = filter;
    this.indent = indent;
    this.refFile = requireNonNull(refFile, "refFile");
    this.logFile = logFile;
    this.modCountAtLastWrite = 0;
    this.modCount = 0;

    // Load the document.
    try {
      DocumentBuilder docBuilder =
          DOCUMENT_BUILDER_FACTORY.get().newDocumentBuilder();
      try (InputStream inputStream = refFile.openStream()) {
        // Parse the reference file.
        this.doc = docBuilder.parse(inputStream);
        // Don't write a log file yet -- as far as we know, it's still identical.
      } catch (IOException e) {
        // There's no reference file. Create and write a log file.
        this.doc = docBuilder.newDocument();
        this.doc.appendChild(doc.createElement(ROOT_TAG));
        flushDoc();
      }
      this.root = doc.getDocumentElement();
      this.xmlTestCases = analyze(this.root);
      existsMethodOnlyInXml = checkExists(this.root, javaTestMethods, this.xmlTestCases);
      outOfOrderTests = validateOrder(this.root, this.xmlTestCases);
    } catch (ParserConfigurationException | SAXException e) {
      throw new RuntimeException("error while creating xml parser", e);
    }
  }

  //~ Methods ----------------------------------------------------------------

  public void checkActualAndReferenceFiles() {
    if (existsMethodOnlyInXml) {
      modCount++;
      flushDoc();
    }

    if (!logFile.exists()) {
      return;
    }

    final String resourceFile =
        Sources.of(refFile).file().getPath().replace(
            String.join(File.separator, "build", "resources", "test"),
            String.join(File.separator, "src", "test", "resources"));

    final String diff = DiffTestCase.diff(new File(resourceFile), logFile);

    if (!diff.isEmpty()) {
      throw new IllegalArgumentException("Actual and reference files differ. "
          + "If you are adding new tests, replace the reference file with the "
          + "current actual file, after checking its content."
          + "\ndiff " + logFile.getAbsolutePath() + " " + resourceFile + "\n"
          + diff);
    }
  }

  String logFilePath() {
    return logFile.getAbsolutePath();
  }

  private static URL findFile(Class<?> clazz, final String suffix) {
    // The reference file for class "com.foo.Bar" is "com/foo/Bar.xml"
    String rest = "/" + clazz.getName().replace('.', File.separatorChar)
        + suffix;
    return requireNonNull(clazz.getResource(rest));
  }

  /** Returns the diff repository, checking that it is not null.
   *
   * <p>If it is null, throws {@link IllegalArgumentException} with a message
   * informing people that they need to change their test configuration. */
  public static DiffRepository castNonNull(
      @Nullable DiffRepository diffRepos) {
    if (diffRepos != null) {
      return Nullness.castNonNull(diffRepos);
    }
    throw new IllegalArgumentException("diffRepos is null; if you require a "
        + "DiffRepository, set it in your test's fixture() method");
  }

  /**
   * Expands a string containing one or more variables. (Currently only works
   * if there is one variable.)
   */
  public String expand(String tag, String text) {
    requireNonNull(tag, "tag");
    requireNonNull(text, "text");
    if (text.startsWith("${")
        && text.endsWith("}")) {
      final String testCaseName = getCurrentTestCaseName();
      final String token = text.substring(2, text.length() - 1);
      assert token.startsWith(tag) : "token '" + token
          + "' does not match tag '" + tag + "'";
      String expanded = get(testCaseName, token);
      if (expanded == null) {
        // Token is not specified. Return the original text: this will
        // cause a diff, and the actual value will be written to the
        // log file.
        return text;
      }
      if (filter != null) {
        expanded = filter.filter(this, testCaseName, tag, text, expanded);
      }
      return expanded;
    } else {
      // Make sure what appears in the resource file is consistent with
      // what is in the Java. It helps to have a redundant copy in the
      // resource file.
      final String testCaseName = getCurrentTestCaseName();
      if (baseRepository == null
          || baseRepository.get(testCaseName, tag) == null) {
        set(tag, text);
      }
      return text;
    }
  }

  /**
   * Sets the value of a given resource of the current test case.
   *
   * @param resourceName Name of the resource, e.g. "sql"
   * @param value        Value of the resource
   */
  public synchronized void set(String resourceName, String value) {
    requireNonNull(resourceName, "resourceName");
    final String testCaseName = getCurrentTestCaseName();
    update(testCaseName, resourceName, value);
  }

  public void amend(String expected, String actual) {
    if (expected.startsWith("${")
        && expected.endsWith("}")) {
      String token = expected.substring(2, expected.length() - 1);
      set(token, actual);
    }
  }

  /**
   * Returns a given resource from a given test case.
   *
   * @param testCaseName Name of test case, e.g. "testFoo"
   * @param resourceName Name of resource, e.g. "sql", "plan"
   * @return The value of the resource, or null if not found
   */
  private synchronized @Nullable String get(
      final String testCaseName,
      String resourceName) {
    Element testCaseElement = getTestCaseElement(testCaseName, true, null);
    if (testCaseElement == null) {
      if (baseRepository != null) {
        return baseRepository.get(testCaseName, resourceName);
      } else {
        return null;
      }
    }
    final @Nullable Element resourceElement =
        getResourceElement(testCaseElement, resourceName);
    if (resourceElement != null) {
      return getText(resourceElement);
    }
    return null;
  }

  /**
   * Returns the text under an element.
   */
  private static String getText(Element element) {
    // If there is a <![CDATA[ ... ]]> child, return its text and ignore
    // all other child elements.
    final NodeList childNodes = element.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node node = childNodes.item(i);
      if (node instanceof CDATASection) {
        return node.getNodeValue();
      }
    }

    // Otherwise return all the text under this element (including
    // whitespace).
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node node = childNodes.item(i);
      if (node instanceof Text) {
        buf.append(((Text) node).getWholeText());
      }
    }
    return buf.toString();
  }

  /**
   * Returns the &lt;TestCase&gt; element corresponding to the current test
   * case.
   *
   * @param testCaseName  Name of test case
   * @param checkOverride Make sure that if an element overrides an element in
   *                      a base repository, it has overrides="true"
   * @return TestCase element, or null if not found
   */
  private synchronized @Nullable Element getTestCaseElement(
      final String testCaseName,
      boolean checkOverride,
      @Nullable List<Pair<String, Element>> elements) {
    final NodeList childNodes = root.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node child = childNodes.item(i);
      if (child.getNodeName().equals(TEST_CASE_TAG)) {
        Element testCase = (Element) child;
        final String name = testCase.getAttribute(TEST_CASE_NAME_ATTR);
        if (testCaseName.equals(name)) {
          if (checkOverride
              && (baseRepository != null)
              && (baseRepository.getTestCaseElement(testCaseName, false, null) != null)
              && !"true".equals(
                  testCase.getAttribute(TEST_CASE_OVERRIDES_ATTR))) {
            throw new RuntimeException(
                "TestCase  '" + testCaseName + "' overrides a "
                + "test case in the base repository, but does "
                + "not specify 'overrides=true'");
          }
          if (outOfOrderTests.contains(testCaseName)) {
            ++modCount;
            flushDoc();
            throw new IllegalArgumentException("TestCase '" + testCaseName
                + "' is out of order in the reference file: "
                + Sources.of(refFile).file() + "\n"
                + "To fix, copy the generated log file: " + logFile + "\n");
          }
          return testCase;
        }
        if (elements != null) {
          elements.add(Pair.of(name, testCase));
        }
      }
    }
    return null;
  }

  /**
   * Returns the name of the current test case by looking up the call stack for
   * a method whose name starts with "test", for example "testFoo".
   *
   * @param fail Whether to fail if no method is found
   * @return Name of current test case, or null if not found
   */
  private static @Nullable String getCurrentTestCaseName(boolean fail) {
    // REVIEW jvs 12-Mar-2006: Too clever by half.  Someone might not know
    // about this and use a private helper method whose name also starts
    // with test. Perhaps just require them to pass in getName() from the
    // calling TestCase's setUp method and store it in a thread-local,
    // failing here if they forgot?

    // Clever, this. Dump the stack and look up it for a method which
    // looks like a test case name, e.g. "testFoo".
    final StackTraceElement[] stackTrace;
    Throwable runtimeException = new Throwable();
    runtimeException.fillInStackTrace();
    stackTrace = runtimeException.getStackTrace();
    for (StackTraceElement stackTraceElement : stackTrace) {
      final String methodName = stackTraceElement.getMethodName();
      if (methodName.startsWith("test")) {
        return methodName;
      }
    }
    if (fail) {
      throw new RuntimeException("no test case on current call stack");
    } else {
      return null;
    }
  }

  /** Returns the current test case name;
   * equivalent to {@link #getCurrentTestCaseName}(true),
   * this method throws if not found, and never returns null. */
  private static String getCurrentTestCaseName() {
    return requireNonNull(getCurrentTestCaseName(true));
  }

  public void assertEquals(String tag, String expected, String actual) {
    final String testCaseName = getCurrentTestCaseName(true);
    String expected2 = expand(tag, expected);
    if (expected2 == null) {
      update(testCaseName, expected, actual);
      throw new AssertionError("reference file does not contain resource '"
          + expected + "' for test case '" + testCaseName + "'");
    } else {
      try {
        // TODO jvs 25-Apr-2006:  reuse bulk of
        // DiffTestCase.diffTestLog here; besides newline
        // insensitivity, it can report on the line
        // at which the first diff occurs, which is useful
        // for largish snippets
        String expected2Canonical =
            expected2.replace(Util.LINE_SEPARATOR, "\n");
        String actualCanonical =
            actual.replace(Util.LINE_SEPARATOR, "\n");
        Assertions.assertEquals(expected2Canonical, actualCanonical, tag);
      } catch (AssertionFailedError e) {
        amend(expected, actual);
        throw e;
      }
    }
  }

  /**
   * Creates a new document with a given resource.
   *
   * <p>This method is synchronized, in case two threads are running test
   * cases of this test at the same time.
   *
   * @param testCaseName Test case name
   * @param resourceName Resource name
   * @param value        New value of resource
   */
  private synchronized void update(
      String testCaseName,
      String resourceName,
      String value) {
    final List<Pair<String, Element>> map = new ArrayList<>();
    Element testCaseElement = getTestCaseElement(testCaseName, true, map);
    if (testCaseElement == null) {
      testCaseElement = doc.createElement(TEST_CASE_TAG);
      testCaseElement.setAttribute(TEST_CASE_NAME_ATTR, testCaseName);
      Node refElement = ref(testCaseName, map);
      root.insertBefore(testCaseElement, refElement);
      ++modCount;
    }
    Element resourceElement =
        getResourceElement(testCaseElement, resourceName, true);
    if (resourceElement == null) {
      resourceElement = doc.createElement(RESOURCE_TAG);
      resourceElement.setAttribute(RESOURCE_NAME_ATTR, resourceName);
      testCaseElement.appendChild(resourceElement);
      ++modCount;
      if (!value.isEmpty()) {
        resourceElement.appendChild(doc.createCDATASection(value));
      }
    } else {
      final List<Node> newChildList;
      if (value.isEmpty()) {
        newChildList = ImmutableList.of();
      } else {
        newChildList = ImmutableList.of(doc.createCDATASection(value));
      }
      if (replaceChildren(resourceElement, newChildList)) {
        ++modCount;
      }
    }

    // Write out the document.
    flushDoc();
  }

  private static @Nullable Node ref(String testCaseName,
      List<Pair<String, Element>> map) {
    if (map.isEmpty()) {
      return null;
    }
    // Compute the position that the new element should be if the map were
    // sorted.
    int i = 0;
    final List<String> names = Pair.left(map);
    for (String s : names) {
      if (s.compareToIgnoreCase(testCaseName) <= 0) {
        ++i;
      }
    }
    // Starting at a proportional position in the list,
    // move forwards through lesser names, then
    // move backwards through greater names.
    //
    // The intended effect is that if the list is already sorted, the new item
    // will end up in exactly the right position, and if the list is not sorted,
    // the new item will end up in approximately the right position.
    while (i < map.size()
        && names.get(i).compareToIgnoreCase(testCaseName) < 0) {
      ++i;
    }
    if (i >= map.size() - 1) {
      return null;
    }
    while (i >= 0 && names.get(i).compareToIgnoreCase(testCaseName) > 0) {
      --i;
    }
    return map.get(i + 1).right;
  }

  /**
   * Flushes the reference document to the file system.
   */
  private synchronized void flushDoc() {
    if (modCount == modCountAtLastWrite) {
      // Document has not been modified since last write.
      return;
    }
    try {
      boolean b = logFile.getParentFile().mkdirs();
      Util.discard(b);
      try (Writer w = Util.printWriter(logFile)) {
        write(doc, w, indent);
      }
    } catch (IOException e) {
      throw Util.throwAsRuntime("error while writing test reference log '"
          + logFile + "'", e);
    }
    modCountAtLastWrite = modCount;
  }

  /** Analyzes the root element.
   *
   * <p>Returns the set of test names that in the reference file. */
  private static SortedMap<String, Node> analyze(Element root) {
    if (!root.getNodeName().equals(ROOT_TAG)) {
      throw new RuntimeException("expected root element of type '" + ROOT_TAG
          + "', but found '" + root.getNodeName() + "'");
    }

    // Make sure that there are no duplicate test cases, and count how many
    // tests are out of order.
    final SortedMap<String, Node> testCases = new TreeMap<>();
    final NodeList childNodes = root.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node child = childNodes.item(i);
      if (child.getNodeName().equals(TEST_CASE_TAG)) {
        Element testCase = (Element) child;
        final String name = testCase.getAttribute(TEST_CASE_NAME_ATTR);
        if (testCases.put(name, testCase) != null) {
          throw new RuntimeException("TestCase '" + name + "' is duplicate");
        }
      }
    }
    return testCases;
  }

  /** Checks if there are methods that only exist in XML.
   *
   * <p>Returns true if there are methods that only exist in XML. */
  private static Boolean checkExists(Element root, Set<String> javaTestMethods,
      Map<String, Node> testCases) {
    final List<String> existsOnlyInXml = new ArrayList<>();
    for (Map.Entry<String, Node> entry : testCases.entrySet()) {
      String name = entry.getKey();
      if (!javaTestMethods.contains(name)) {
        existsOnlyInXml.add(name);
      }
    }
    if (!existsOnlyInXml.isEmpty()) {
      for (String value : existsOnlyInXml) {
        root.removeChild(testCases.get(value));
      }
    }

    return !existsOnlyInXml.isEmpty();
  }

  /** Validates the root element order.
   *
   * <p>Returns the set of test names that are out of order in the reference
   * file (empty if the reference file is fully sorted). */
  private static ImmutableSortedSet<String> validateOrder(Element root,
      SortedMap<String, Node> testCases) {
    String previousName = null;
    final List<String> outOfOrderNames = new ArrayList<>();
    for (Map.Entry<String, Node> entry : testCases.entrySet()) {
      String name = entry.getKey();
      if (previousName != null && previousName.compareTo(name) > 0) {
        outOfOrderNames.add(name);
      }
      previousName = name;
    }

    // If any nodes were out of order, rebuild the document in sorted order.
    if (!outOfOrderNames.isEmpty()) {
      for (Node testCase : testCases.values()) {
        root.removeChild(testCase);
      }
      for (Node testCase : testCases.values()) {
        root.appendChild(testCase);
      }
    }
    return ImmutableSortedSet.copyOf(outOfOrderNames);
  }

  /**
   * Returns a given resource from a given test case.
   *
   * @param testCaseElement The enclosing TestCase element, e.g. <code>
   *                        &lt;TestCase name="testFoo"&gt;</code>.
   * @param resourceName    Name of resource, e.g. "sql", "plan"
   * @return The value of the resource, or null if not found
   */
  private static @Nullable Element getResourceElement(
      Element testCaseElement,
      String resourceName) {
    return getResourceElement(testCaseElement, resourceName, false);
  }

  /**
   * Returns a given resource from a given test case.
   *
   * @param testCaseElement The enclosing TestCase element, e.g. <code>
   *                        &lt;TestCase name="testFoo"&gt;</code>.
   * @param resourceName    Name of resource, e.g. "sql", "plan"
   * @param killYoungerSiblings Whether to remove resources with the same
   *                        name and the same parent that are eclipsed
   * @return The value of the resource, or null if not found
   */
  private static @Nullable Element getResourceElement(Element testCaseElement,
      String resourceName, boolean killYoungerSiblings) {
    final NodeList childNodes = testCaseElement.getChildNodes();
    Element found = null;
    final List<Node> kills = new ArrayList<>();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node child = childNodes.item(i);
      if (child.getNodeName().equals(RESOURCE_TAG)
          && resourceName.equals(
              ((Element) child).getAttribute(RESOURCE_NAME_ATTR))) {
        if (found == null) {
          found = (Element) child;
        } else if (killYoungerSiblings) {
          kills.add(child);
        }
      }
    }
    for (Node kill : kills) {
      testCaseElement.removeChild(kill);
    }
    return found;
  }

  private static void removeAllChildren(Element element) {
    final NodeList childNodes = element.getChildNodes();
    while (childNodes.getLength() > 0) {
      element.removeChild(childNodes.item(0));
    }
  }

  private static boolean replaceChildren(Element element, List<Node> children) {
    // Current children
    final NodeList childNodes = element.getChildNodes();
    final List<Node> list = new ArrayList<>();
    for (Node item : iterate(childNodes)) {
      if (item.getNodeType() != Node.TEXT_NODE) {
        list.add(item);
      }
    }

    // Are new children equal to old?
    if (equalList(children, list)) {
      return false;
    }

    // Replace old children with new children
    removeAllChildren(element);
    children.forEach(element::appendChild);
    return true;
  }

  /** Returns whether two lists of nodes are equal. */
  private static boolean equalList(List<Node> list0, List<Node> list1) {
    return list1.size() == list0.size()
        && Pair.zip(list1, list0).stream()
        .allMatch(p -> p.left.isEqualNode(p.right));
  }

  /**
   * Serializes an XML document as text.
   *
   * <p>FIXME: I'm sure there's a library call to do this, but I'm danged if I
   * can find it. -- jhyde, 2006/2/9.
   */
  private static void write(Document doc, Writer w, int indent) {
    final XmlOutput out = new XmlOutput(w);
    out.setGlob(true);
    out.setIndentString(Spaces.of(indent));
    writeNode(doc, out);
  }

  private static void writeNode(Node node, XmlOutput out) {
    final NodeList childNodes;
    switch (node.getNodeType()) {
    case Node.DOCUMENT_NODE:
      out.print("<?xml version=\"1.0\" ?>\n");
      childNodes = node.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
        Node child = childNodes.item(i);
        writeNode(child, out);
      }
      break;

    case Node.ELEMENT_NODE:
      Element element = (Element) node;
      final String tagName = element.getTagName();
      out.beginBeginTag(tagName);

      // Attributes.
      final NamedNodeMap attributeMap = element.getAttributes();
      for (int i = 0; i < attributeMap.getLength(); i++) {
        final Node att = attributeMap.item(i);
        out.attribute(
            att.getNodeName(),
            att.getNodeValue());
      }
      out.endBeginTag(tagName);

      // Write child nodes, ignoring attributes but including text.
      childNodes = node.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
        Node child = childNodes.item(i);
        if (child.getNodeType() == Node.ATTRIBUTE_NODE) {
          continue;
        }
        writeNode(child, out);
      }
      out.endTag(tagName);
      break;

    case Node.ATTRIBUTE_NODE:
      out.attribute(
          node.getNodeName(),
          node.getNodeValue());
      break;

    case Node.CDATA_SECTION_NODE:
      CDATASection cdata = (CDATASection) node;
      out.cdata(
          cdata.getNodeValue(),
          true);
      break;

    case Node.TEXT_NODE:
      Text text = (Text) node;
      final String wholeText = text.getNodeValue();
      if (!isWhitespace(wholeText)) {
        out.cdata(wholeText, false);
      }
      break;

    case Node.COMMENT_NODE:
      Comment comment = (Comment) node;
      out.print("<!--" + comment.getNodeValue() + "-->\n");
      break;

    default:
      throw new RuntimeException("unexpected node type: " + node.getNodeType()
          + " (" + node + ")");
    }
  }

  private static boolean isWhitespace(String text) {
    for (int i = 0, count = text.length(); i < count; ++i) {
      final char c = text.charAt(i);
      switch (c) {
      case ' ':
      case '\t':
      case '\n':
        break;
      default:
        return false;
      }
    }
    return true;
  }

  /**
   * Finds the repository instance for a given class, with no base
   * repository or filter.
   *
   * @param clazz Test case class
   * @return The diff repository shared between test cases in this class.
   */
  public static DiffRepository lookup(Class<?> clazz) {
    return lookup(clazz, null, null, 2);
  }

  /**
   * Finds the repository instance for a given class.
   *
   * <p>It is important that all test cases in a class share the same
   * repository instance. This ensures that, if two or more test cases fail,
   * the log file will contains the actual results of both test cases.
   *
   * <p>The <code>baseRepository</code> parameter is useful if the test is an
   * extension to a previous test. If the test class has a base class which
   * also has a repository, specify the repository here. DiffRepository will
   * look for resources in the base class if it cannot find them in this
   * repository. If test resources from test cases in the base class are
   * missing or incorrect, it will not write them to the log file -- you
   * probably need to fix the base test.
   *
   * <p>Use the <code>filter</code> parameter if you expect the test to
   * return results slightly different than in the repository. This happens
   * if the behavior of a derived test is slightly different than a base
   * test. If you do not specify a filter, no filtering will happen.
   *
   * @param clazz     Test case class
   * @param baseRepository Base repository
   * @param filter    Filters each string returned by the repository
   * @param indent    Indent of the XML file (usually 2)
   *
   * @return The diff repository shared between test cases in this class
   */
  public static DiffRepository lookup(Class<?> clazz,
      @Nullable DiffRepository baseRepository, @Nullable Filter filter,
      int indent) {
    final Key key = new Key(clazz, baseRepository, filter, indent);
    return REPOSITORY_CACHE.getUnchecked(key);
  }

  /**
   * Callback to filter strings before returning them.
   */
  public interface Filter {
    /**
     * Filters a string.
     *
     * @param diffRepository Repository
     * @param testCaseName   Test case name
     * @param tag            Tag being expanded
     * @param text           Text being expanded
     * @param expanded       Expanded text
     * @return Expanded text after filtering
     */
    String filter(
        DiffRepository diffRepository,
        String testCaseName,
        String tag,
        String text,
        String expanded);
  }

  /** Cache key. */
  private static class Key {
    private final Class<?> clazz;
    private final @Nullable DiffRepository baseRepository;
    private final @Nullable Filter filter;
    private final int indent;

    Key(Class<?> clazz, @Nullable DiffRepository baseRepository,
        @Nullable Filter filter, int indent) {
      this.clazz = requireNonNull(clazz, "clazz");
      this.baseRepository = baseRepository;
      this.filter = filter;
      this.indent = indent;
    }

    @Override public int hashCode() {
      return Objects.hash(clazz, baseRepository, filter);
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj instanceof Key
          && clazz.equals(((Key) obj).clazz)
          && Objects.equals(baseRepository, ((Key) obj).baseRepository)
          && Objects.equals(filter, ((Key) obj).filter);
    }

    DiffRepository toRepo() {
      final URL refFile = findFile(clazz, ".xml");
      final String refFilePath = Sources.of(refFile).file().getAbsolutePath();
      final String logFilePath = refFilePath
          .replace("resources", "diffrepo")
          .replace(".xml", "_actual.xml");
      final File logFile = new File(logFilePath);
      assert !refFilePath.equals(logFile.getAbsolutePath());
      Set<String> javaTestMethods =
          Arrays.stream(clazz.getDeclaredMethods()).map(Method::getName)
              .filter(name -> name.startsWith("test")).collect(Collectors.toSet());
      return new DiffRepository(refFile, logFile, baseRepository, filter,
          indent, javaTestMethods);
    }
  }

  private static Iterable<Node> iterate(NodeList nodeList) {
    return new AbstractList<Node>() {
      @Override public Node get(int index) {
        return nodeList.item(index);
      }

      @Override public int size() {
        return nodeList.getLength();
      }
    };
  }
}
