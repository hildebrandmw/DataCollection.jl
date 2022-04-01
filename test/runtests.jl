using DataCollection
using Test

using DataFrames
import DataStructures: SortedDict

#####
##### Test behavior of `lower`.
#####

@testset "Testing Helper Functions" begin
    @test DataCollection.lower(1) === 1

    # Functions
    @test DataCollection.lower(identity) == "identity"
    @test DataCollection.lower(cos) == "cos"
    @test DataCollection.lower([1,2,3]) == [1,2,3]
    @test DataCollection.lower([1, identity, cos, 4]) == [1, "identity", "cos", 4]
end

struct ParametersA <: AbstractParameters
    a::Int
    b::Int
end

struct ParametersB <: AbstractParameters
    c::Int
    d::Int
end

struct ParametersC <: AbstractParameters
    e::Int
    f::Int
end

struct ParametersError <: AbstractParameters
    e::Int
    # Repeat `a` so we can test errors during merging
    a::Int
end

struct DataA <: AbstractData
    data::Int
end

@testset "Testing Parameters" begin
    x = ParametersA(1,2)
    @test DataCollection.lower(x) == SortedDict(:a => 1, :b => 2)
    @test DataCollection.dict(x) == SortedDict(:a => 1, :b => 2)

    y = ParametersB(3,4)
    expected = SortedDict(
        :a => 1,
        :b => 2,
        :c => 3,
        :d => 4,
    )
    @test DataCollection._lower(x,y) == expected

    z = ParametersError(5,6)
    @test_throws ErrorException DataCollection._lower(x,y,z)

    # DataFrames schenanigans
    df = DataFrame()

    x = ParametersA(1, 2)
    y = ParametersB(3, 4)
    _y = ParametersB(4, 4)
    d = DataA(10)

    # Create a row for these parameters and data
    addrow!(df, d, x, y)
    @test nrow(df) == 1
    @test DataCollection.hasrow(df, x, y)

    # Ensure that we don't get false positives
    @test !DataCollection.hasrow(df, x, _y)

    # Get this row and make sure the data propogated properly.
    row = df[1, :]
    @test row.data == d.data

    # Try updating.
    @test_throws ErrorException addrow!(df, d, x, y)
    d2 = DataA(20)
    addrow!(df, d2, x, y; force = true)
    row = df[1, :]
    @test row.data == 20

    # Now, add another row
    x2 = ParametersA(10, 20)
    y2 = ParametersB(30, 40)
    addrow!(df, d2, x2, y2)
    @test nrow(df) == 2
    @test DataCollection.hasrow(df, x2, y2)
    @test DataCollection.hasrow(df, x, y)

    # Add something with different parameters.
    # Make sure we get an error if we don't pass `cols = :union`
    z = ParametersC(1,2)
    @test_throws ArgumentError addrow!(df, d, z)
    addrow!(df, d, z; cols = :union)

    @test nrow(df) == 3

    # We should still have the rows we added earlier
    @test DataCollection.hasrow(df, x2, y2)
    @test DataCollection.hasrow(df, x, y)
    @test DataCollection.hasrow(df, z)
end

