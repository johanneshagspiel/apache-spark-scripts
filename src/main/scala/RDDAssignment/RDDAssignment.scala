package RDDAssignment

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {

  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = {
    commits.count()
  }

  /**
    * We want to know how often programming languages are used in committed files. We require a RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
    */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {

    def getFileExtension(input: Commit): List[String] = {
      val tempFiles: List[File] = input.files
      getFileExtension2(tempFiles)
    }

    def getFileExtension2(input: List[File]): List[String] = input match {
      case Nil => Nil
      case x :: tail => getFileExtension3(x.filename.get) :: getFileExtension2(tail)
    }

    def getFileExtension3(input: String): String = {
        val pattern = """[^\.]*$""".r
        val tempFileType: Option[String] = pattern.findFirstIn(input)
        if(tempFileType.get == input || tempFileType.isEmpty) "unknown" else tempFileType.get
    }

    val result = commits.
      flatMap(commit => getFileExtension(commit)).
      map(entry => (entry, 1.toLong)).
      reduceByKey((x, y) => x + y)

    result
  }

  /**
    * Competitive users on Github might be interested in their ranking in number of commits. We require as return a
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit authors name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing commit author names and total count of commits done by the author, in ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {

    def flattenTemp[A, B, C](x:(A,(B, C))): (A,B,C) = (x._1, x._2._1, x._2._2)

    val pipeline = commits.
        map(commit => commit.commit.author.name).
        map(name => (name, 1.toLong)).
        reduceByKey((name , occurences) => name + occurences).
        sortBy(x => x._1.toLowerCase, true).
        sortBy(x => x._2, false).
        zipWithIndex().
        map(_.swap).
        map(element => flattenTemp(element))

    pipeline
    }

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we an RDD that
    * contains the committer name and the total of their commits. As stats are optional, missing Stat cases should be
    * handles as s"Stat(0, 0, 0)". If an User is given that is not in the dataset, then the username should not occur in
    * the return RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {

    val pipeline = commits.
      filter(commit => users.contains(commit.commit.committer.name)).
      map(commit => (commit.commit.committer.name, commit.stats.getOrElse(Stats(0,0,0)))).
      reduceByKey((stats1, stats2) => Stats(stats1.total + stats2.total, stats1.additions + stats2.additions, stats1.deletions + stats2.deletions))

    pipeline
  }


  /**
    * There are different types of people, those who own repositories, and those who make commits. Although Git blame is
    * excellent in finding these types of people, we want to do it in Spark. We require as output an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given RDD. Note that the repository owner is contained within Github urls.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the username that have either only committed to repositories or only own
    *         repositories.
    */
  def assignment_5(commits: RDD[Commit]): RDD[String] = {

    def helperGetRepositoryOwner(urlIn: String): String = {
      val pattern = """(/repos)(/.*)""".r
      val fileExtensionTemp: Option[String] = pattern.findFirstIn(urlIn)
      val fileExtension = fileExtensionTemp.getOrElse("SomethingWentWrong").substring(7)
      val fileSplit = fileExtension.split("/")
      val fileResult = fileSplit(0)
      fileResult
    }

    val ownerRepository = commits.map(commit => commit.url).map(url => helperGetRepositoryOwner(url))
    val commitRepository = commits.map(commit => commit.commit.author.name)
    val intersection = ownerRepository.intersection(commitRepository)
    val union = ownerRepository.union(commitRepository)
    val result = union.subtract(intersection).distinct()

    result
  }

  /**
    * Sometimes developers make mistakes, sometimes they make many. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit. Note that for a commit to be eligible for a 'commit streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    * We require as return a RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest streak of an user and how often said streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuple type containing a commit author username, and a tuple containing the length of the longest
    *         commit streak as well its frequency.
    */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {

    def helper(commit: Commit): (String, Int) = {
      val name = commit.commit.author.name
      val Streak = helperRevertStreak(commit.commit.message)
      (name, Streak)
    }

    def helperRevertStreak(input: String): Int = {

        if (input.startsWith("Revert"))
        {
          val pattern = """Revert""".r
          val temp = pattern.findAllIn(input).size
          temp
        }
      else
    {
        0
      }

    }

      val pipeline = commits.
        map(commit => (helper(commit))).
        map(element => (element._1, (element._2, 1))).
        reduceByKey((x,y) => ((if (x._1 > y._1) (x._1, x._2) else if (x._1 == y._1) (x._1, x._2 + 1) else (y._1, y._2)))).
        filter(x => (x._2._1 !=0 ))

    pipeline
  }


  /**
    * We want to know the number of commits that are made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to the repository. Note that from
    * this exercise on, expensive functions like groupBy are no longer allowed to be used. In real life these wide
    * dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDD's.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing a tuple indicating the repository name, the number of commits made to the repository as
    *         well as the unique committer usernames that committed to the repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {

    def flattenTemp1[A, B, C](x:(A,(B, C))): (A,C,B) = (x._1, x._2._2, x._2._1)
    def flattenTemp2[A, B, C](x:((A,B), C)): (A,(B,C)) = (x._1._1, (x._1._2, x._2))

    def helperGetRepositoryName(urlIn: String): String = {
      val pattern = """(/repos)(/.*)""".r
      val repositoryNameTemp: Option[String] = pattern.findFirstIn(urlIn)
      val repositoryName = repositoryNameTemp.getOrElse("SomethingWentWrong").substring(7)
      val repositorySplit = repositoryName.split("/")
      val repositoryResult = repositorySplit(1)
      repositoryResult
    }

    def helperGetContributer(commit: Commit): Iterable[String] = {
      val test: List[String] = commit.commit.committer.name :: Nil
      test
    }

    def helper(input: Commit): (String, Iterable[String]) = {
      val repoName: String = helperGetRepositoryName(input.url)
      val contributers: Iterable[String] = helperGetContributer(input)
      val result = (repoName, contributers)
      result
    }

    def helperDistinct(stringIn: String, longIn: Long, listIn: Iterable[String]):(String, Long, Iterable[String]) = {
      val result = (stringIn, longIn, listIn.toList.distinct)
      result
    }

      val pipeline = commits.
        map(commit => helper(commit)).
        map(entry => (entry, 1.toLong)).
        map(x => flattenTemp2(x)).
        reduceByKey((x, y) => (x._1 ++ y._1,x._2 + y._2)).
        map(x => flattenTemp1(x)).
        map(x =>helperDistinct(x._1, x._2, x._3))

    pipeline
  }

  /**
    * Return RDD of tuples containing the repository name and all the files that are contained in that repository.
    * Note that the file names must be unique, so if files occur multiple times (for example due to removal, or new
    * additions), the newest File object must be returned. As the files' filenames are an `Option[String]` discard the
    * files that do not have a filename.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {

    def flattenTemp1[A, B, C, D](x:(A,B, C, D)): (C,(A, B, D)) = (x._3, (x._1, x._2, x._4))
    def flattenTemp2[A, B, C, D](x:(A,(B, C, D))): (B,(A, C)) = (x._2._1, (x._1, x._2._2))
    def flattenTemp3[A, B, C](x:(A,(B, C))): (A, C) = (x._1, x._2._2)

    def helper(commit: Commit): List[(String, Iterable[File], String, Timestamp)] = {

      val  repositoryName = helperGetRepositoryName(commit.url)
      val timestamp: Timestamp = commit.commit.author.date

      val files: Iterable[File] =  helperDeleteFileWithNoName(commit.files)
      val result = helperGetFileLevel(repositoryName, files, timestamp)

      result
    }

    def helperGetFileLevel(repositoryName: String, files: Iterable[File], date: Timestamp): List[(String, Iterable[File], String, Timestamp)] = files match {
      case Nil => Nil
      case x::tail => ((repositoryName, helperList(x), x.filename.get, date)) :: helperGetFileLevel(repositoryName, tail, date)
    }

    def helperList(fileIn: File): Iterable[File] = {
      val test: List[File] = fileIn :: Nil
      test
    }

    def helperGetRepositoryName(urlIn: String): String = {
      val pattern = """(/repos)(/.*)""".r
      val repositoryNameTemp: Option[String] = pattern.findFirstIn(urlIn)
      val repositoryName = repositoryNameTemp.getOrElse("SomethingWentWrong").substring(7)
      val repositorySplit = repositoryName.split("/")
      val repositoryResult = repositorySplit(1)
      repositoryResult
    }

    def helperDeleteFileWithNoName(files: List[File]): List[File] = files match {
      case Nil => Nil
      case head :: tail =>
        if (head.filename.isEmpty) helperDeleteFileWithNoName(tail) else head :: helperDeleteFileWithNoName(tail)
    }

    val pipeline = commits.
      flatMap(x => helper(x)).
      map(x => flattenTemp1(x)).
      reduceByKey((x,y) => (if(x._3.after(y._3)) x else y)).
      map(x => flattenTemp2(x)).
      reduceByKey((x,y) => (x._1, x._2 ++ y._2)).
      map(x => flattenTemp3(x))

    pipeline
  }


  /**
    * For this assignment you are asked to find all the files of a single repository. This in order to create an
    * overview of each files, do this by creating a tuple containing the file name, all corresponding commit SHA's
    * as well as a Stat object representing the changes made to the file.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {

    def flattenTemp1[A, B, C](x:(A, B, C)): (A,(B, C)) = (x._1,(x._2, x._3))
    def flattenTemp2[A, B, C](x:(A, B, C)): (A, B, C) = (x._1,x._2, x._3)


    def helperGetRepository(urlIn: String, repository: String): Boolean = {
      val pattern = """(/repos)(/.*)""".r
      val repositoryNameTemp: Option[String] = pattern.findFirstIn(urlIn)
      val repositoryName = repositoryNameTemp.getOrElse("SomethingWentWrong").substring(7)
      val repositorySplit = repositoryName.split("/")
      val repositoryResult = repositorySplit(1)

      if(repositoryResult.equals(repository)) true else false
    }

    def helper(commit: Commit): List[(String, Seq[String], Stats)] = {
      val files = helperGetFile(commit.files)
      val sha = commit.sha

      val result =helperGetToLevelFile(files, sha)
      result
    }

    def helperGetFile(files: List[File]): List[File] = files match {
      case Nil => Nil
      case head :: tail =>
        if (head.filename.isEmpty) helperGetFile(tail) else head::helperGetFile(tail)
    }

    def helperGetToLevelFile(files: List[File], sha: String): List[(String, Seq[String], Stats)] = files match {
      case Nil => Nil
      case x :: tail => (x.filename.get, helperSeq(sha), Stats(x.additions + x.deletions, x.additions, x.deletions)) :: helperGetToLevelFile(tail, sha)
    }

    def helperSeq(stringIn: String): Seq[String] = {
      val result: Seq[String] = stringIn::Nil
      result
    }

    val pipeline = commits.
      filter(commit => helperGetRepository(commit.url, repository)).
      flatMap(entry => helper(entry)).
      map(x => flattenTemp1(x)).
      reduceByKey((x,y) => (x._1++y._1, Stats(x._2.total + y._2.total, x._2.additions + y._2.additions, x._2.deletions + y._2.deletions))).
      map(x => flattenTemp2(x._1, x._2._1, x._2._2))

    pipeline
  }

  /**
    * We want to generate an overview of the work done by an user per repository. For this we request an RDD containing a
    * tuple containing the committer username, repository name and a `Stats` object. The Stats object containing the
    * total number of additions, deletions and total contribution.
    * Note that as Stats are optional, therefore a type of Option[Stat] is required.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples of committer names, repository names and and Option[Stat] representing additions and
    *         deletions.
    */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {

    def flattenTemp[A, B, C](x:((A,B), C)): (A,B,C) = (x._1._1, x._1._2, x._2)

    def helper(commit: Commit): ((String, String), Option[Stats]) = {
      val repositoryName = helperGetRepositoryName(commit.url)
      val commiterName = commit.commit.committer.name
      val stats:Option[Stats]  = commit.stats
      ((commiterName, repositoryName), stats)
    }

    def helperGetRepositoryName(urlIn: String): String = {
      val pattern = """(/repos)(/.*)""".r
      val repositoryNameTemp: Option[String] = pattern.findFirstIn(urlIn)
      val repositoryName = repositoryNameTemp.getOrElse("SomethingWentWrong").substring(7)
      val repositorySplit = repositoryName.split("/")
      val repositoryResult = repositorySplit(1)
      repositoryResult
    }

    val pipeline = commits.
      map(x => (helper(x))).
      reduceByKey((x, y) => (if (x.isEmpty && y.isEmpty) (None) else if (x.isEmpty && y.nonEmpty) y else if (y.isEmpty && x.nonEmpty) x else Some(Stats(x.get.total + y.get.total, x.get.additions + y.get.additions, x.get.deletions + y.get.deletions)))).
      map(x => flattenTemp(x))

    pipeline
  }


  /**
    * Hashing function that computes the md5 hash from a String, which in terms returns a Long to act as a hashing
    * function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB from the inputted String.
    */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * Create a bi-directional graph from committer to repositories, use the md5HashString function above to create unique
    * identifiers for the creation of the graph. This exercise is meant as an extra, and is not mandatory to complete.
    * As the real usage Sparks GraphX library is out of the scope of this course, we will not go further into this, but
    * this can be used for algorithms like PageRank, Hubs and Authorities, clique finding, ect.
    *
    * We expect a node for each repository and each committer (based on committer name). We expect an edge from each
    * committer to the repositories that the developer has committed to.
    *
    * Look into the documentation of Graph and Edge before starting with this (complementatry) exercise.
    * Your vertices should contain information about the type of node, a 'developer' or a 'repository' node.
    * Edges should only exist between repositories and committers.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = ???
}
